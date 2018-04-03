(ns ajure.datalog
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [traversy.lens :as lens]
            [clojure.core.match :as m]))


;{
; :return {:taxon-id ?tid :body-count (count ?tid)}
; :where  {
;          [[?b :body/:id ?bid]
;           [?a :attributes/:body_id ?bid]
;           [?a :attributes [:availability :value] true]
;           [?b :body/taxa ?btaxa]
;           [?btaxa :body.taxa/taxon_id ?tid]
;           [(contains? ?tid #{"id1"})]
;           ]
;          }
; }
(s/def :datalog.query/logical-var (s/and symbol? #(str/starts-with? % "?")))
(s/def :datalog.query/aggregation-function #{'count})
(s/def :datalog.query.where/predicate-function #{'contains?})
(s/def :datalog.query/aggregation (s/cat :fn :datalog.query/aggregation-function
                                         :v :datalog.query/logical-var))
(s/def :datalog.query/return (s/or :s :datalog.query/logical-var
                                   :m (s/map-of any? (s/or :v :datalog.query/logical-var
                                                           :av :datalog.query/aggregation)
                                                :count 2)))

(s/def :datalog.query.where/value (s/or :v :datalog.query/logical-var
                                        :s string?
                                        :vos (s/coll-of string? :kind vector?)))
(s/def :datalog.query.where/predicate (s/spec
                                        (s/cat :fn :datalog.query.where/predicate-function
                                               :args (s/+ :datalog.query.where/value))))
(s/def :datalog.query.where/clause
  (s/and
    vector?
    (s/or :eav (s/cat :e :datalog.query/logical-var
                      :a keyword?
                      :v :datalog.query.where/value)
          :fn (s/cat :predicate :datalog.query.where/predicate))))
(s/def :datalog.query/where (s/coll-of :datalog.query.where/clause))

(s/def :datalog/query (s/keys :req-un [:datalog.query/where (or
                                                              :datalog.query/return
                                                              :datalog.query/update)]))

(defn get-root-e-vertices [v-vertex eav-by-v]
  (loop [v-list [v-vertex]]
    (let [can-go-up? (some #(contains? eav-by-v %) v-list)]
      (if can-go-up?
        (recur (mapcat #(let [parents (get eav-by-v %)]
                          (if (empty? parents)
                            [%]
                            (map :e parents)))
                       v-list))
        v-list))))

(defn v-or-av->v [[type val]]
  (case type
    :v val
    :av (:v val)))

(defn recognize-return-type [q]
  (let [{:keys [return]} (s/conform :datalog/query q)
        [type val] return]
    (cond
      (= type :s) [:return-single val]
      (and (= type :m) (apply = (map v-or-av->v (vals val)))) [:return-aggregate (v-or-av->v
                                                                                   (get-in val [(first (keys val))]))])))


(defn build-query [q]
  (let [{:keys [return where]} (s/conform :datalog/query q)
        grouped-where (lens/update (group-by first where) (lens/*> lens/all-values lens/each)
                                   second)
        eav-by-e (group-by :e (:eav grouped-where))
        eav-by-v (->
                   (group-by :v (filter #(= :v (get-in % [:v 0])) (:eav grouped-where)))
                   (lens/update lens/all-keys second))
        type (m/match [return]
               [[:m {k1 [:v v] k2 [:av {:v v :fn fn}]}]] [:aggregate-fn fn v]
               :else nil)]


    [return
     type
     grouped-where
     eav-by-e
     eav-by-v
     (get-root-e-vertices '?tid eav-by-v)]))

(build-query '{
               :return {:taxon_id ?tid :body_count (count ?tid)}
               :where
                       [[?b :body/id ?bid]
                        [?a :attributes/body_id ?bid]
                        [?a :attributes/availability ?aa]
                        [?aa :attributes.availability/value "true"]
                        [?b :body/taxa ?btaxa]
                        [?btaxa :body.taxa/taxon_id ?tid]
                        [(contains? ?tid ["id1"])]]})





