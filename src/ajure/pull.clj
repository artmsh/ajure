(ns ajure.pull
  (:require [ajure.protocols :refer :all]
            [clojure.spec.alpha :as s]
            [clj-aql.core :refer :all]
            [clojure.string :as str]
            [cheshire.core :as json]))

(s/def ::recursion-limit (s/or :pos-number pos-int? :dots #{"..."}))
(s/def ::limit-expr (s/cat :op #{"limit" 'limit} :attr-name ::attr-name :amount (s/or :pos-number pos-int? :nil nil?)))
(s/def ::default-expr (s/cat :op #{"default"} :attr-name ::attr-name :any any?))
(s/def ::attr-expr (s/or :limit-expr ::limit-expr
                         :default-expr ::default-expr))
(s/def ::attr-name keyword?)
(s/def ::wildcard #{"*" '*'})
(s/def ::map-spec (s/and map? (s/* (s/tuple (s/or :attr-name ::attr-name
                                                  :limit-expr ::limit-expr)
                                            (s/or :pattern ::pattern
                                                  :recursion-limit ::recursion-limit)))))

(s/def ::attr-spec (s/or :attr-name ::attr-name
                         :wildcard ::wildcard
                         :map-spec ::map-spec
                         :attr-expr ::attr-expr))
(s/def ::pattern (s/coll-of ::attr-spec :kind vector?))

(defn reverse-lookup-attr? [attr]
  (str/starts-with? (name attr) "_"))

; (FOR [i] :IN [1]
;  (LET [document (DOCUMENT "location" "t1p")])
;  (LET [_id document.id])
;  (LET [_locationType document.locationType])
;  (LET [__location_id (FOR [a] :IN attributes (FILTER a.location_id==document.id) (LIMIT 10) (RETURN a.id))])
;  (RETURN {id:_id,locationType:_locationType,_location_id:__location_id})
; )

(defn get-ns [kw]
  (first (str/split (subs (str kw) 1) #"/")))

(def counter (atom 0))

(defn gen-arango-var [prefix]
  (let [res (str prefix @counter)]
    (swap! counter inc)
    res))

(defn build-reverse-lookup [kw la return-only-id? doc-var]
  (let [[_ limit-amount] (if (nil? la) [nil nil] la)
        nsp (get-ns kw)
        attr (name kw)
        header (list 'FOR ['o] :IN nsp)
        filter (list 'FILTER (str "o." (subs attr 1)) '== (str doc-var ".id"))
        limit (when-not (nil? limit-amount) (list 'LIMIT limit-amount))
        return (list 'RETURN (if return-only-id? (symbol "o.id") (symbol "o")))]
    (cond-> header
            filter (concat [filter])
            limit (concat [limit])
            return (concat [return]))))

(defn kw->bind [kw] (symbol (gen-arango-var (str "$z" (name kw)))))

(defn build-lookup [kw la doc-var]
  (let [[_ limit-amount] (if (nil? la) [nil nil] la)]
    (list 'LET [(kw->bind kw)
                (if (reverse-lookup-attr? kw)
                  (build-reverse-lookup kw la true doc-var)
                  (str doc-var "." (name kw)))])))

(declare pattern->aql)

(defn attr-spec->aql [[type val] doc-var]
  (case type
    :attr-name (vector (build-lookup val nil doc-var))
    :wildcard []
    :map-spec (map (fn [[key val]]
                     (let [kw (case (first key)
                                :attr-name (second key)
                                :limit-expr (get-in key [1 :attr-name]))
                           limit-amount (when (= (first key) :limit-expr) (get-in key [1 :amount]))
                           nsp (get-ns kw)]
                       (case (first val)
                         :pattern
                         (list 'LET [(kw->bind kw)
                                     (if (reverse-lookup-attr? kw)
                                       (pattern->aql (second val) (build-reverse-lookup kw limit-amount false doc-var))
                                       (pattern->aql (second val) (str doc-var "." (name kw))))])
                         :recursion-limit (build-lookup kw limit-amount doc-var))))
                   val)
    :attr-expr (vector
                 (case (first val)
                   :limit-expr (build-lookup (get-in val [1 :attr-name]) (get-in val [1 :amount]) doc-var)
                   :default-expr (build-lookup (get-in val [1 :attr-name]) nil doc-var)))))

(defn get-attr-name [[type val]]
  (case type
    :attr-name val
    :limit-expr (:attr-name val)))

(defmulti get-attr-names first)
(defmethod get-attr-names :attr-name [[_ kw]] [kw])
(defmethod get-attr-names :wildcard [[_ wk]] ['*])
(defmethod get-attr-names :map-spec [[_ map-spec]] (map (comp get-attr-name first) map-spec))
(defmethod get-attr-names :attr-expr [[_ attr-expr]] [(get-in attr-expr [1 :attr-name])])

(defn remove-last-digits [s]
  (loop [ss s]
    (if (and (not-empty ss) (Character/isDigit (char (last ss))))
      (recur (subs ss 0 (dec (count ss))))
      ss)))

(defn find-binding [kw bind-names]
  (first (filter #(= (name kw) (remove-last-digits (subs (str %) 2))) bind-names)))

; * : (RETURN (MERGE_RECURSIVE document {id:_id, locationType:_locationType}))
(defn build-return-expr [tree doc-var bindings]
  (let [attr-names (mapcat get-attr-names tree)
        bind-names (map (comp first second) bindings)
        _ (prn "bind names: ")
        _ (clojure.pprint/pprint bind-names)
        returned-map (apply hash-map (mapcat #(vector (str "\"" (subs (str %) 1) "\"") (find-binding % bind-names)) attr-names))
        wildcard? (some #{'*} attr-names)]
    (if wildcard?
      (list 'MERGE_RECURSIVE doc-var returned-map)
      returned-map)))

(defn pattern->aql [tree handles]
  (let [doc-var (gen-arango-var "document")
        header (list 'FOR [(symbol doc-var)] :IN handles)
        body (mapcat #(attr-spec->aql % doc-var) tree)]
    (-> header
        (concat body)
        (concat [(list 'RETURN (build-return-expr tree doc-var body))]))))

(defn build-query [tree handles]
  (let [str-handles (json/generate-string handles)
        aql (pattern->aql tree (list 'DOCUMENT (symbol str-handles)))
        ;_ (clojure.pprint/pprint aql)
        exp (macroexpand aql)
        fn (:query exp)
        q (eval fn)
        _ (clojure.pprint/pprint q)]
    {:query q :args (:args exp)}))

(defrecord ArangodbPullApi [api]
  IPull
  (pull [this spec entity-ident]
    (let [pm (pull-many this spec [entity-ident])
          ;_ (clojure.pprint/pprint pm)
          [type val] pm]
      (case type
        :success {:success (first val)}
        :error {:error val})))
  (pull-many [this spec entity-idents]
    (let [eid-grouped (group-by first entity-idents)
          conformed-spec (s/conform ::pattern spec)]
      (reduce (fn [res [db entity-ids]]
                (case (first res)
                  :success (let [cursor (create-cursor api db (build-query conformed-spec (map second entity-ids)) {})
                                 type (first (keys cursor))
                                 val (get cursor type)]
                             (case type
                               :success [:success (concat (second res) (clojure.walk/keywordize-keys val))]
                               :error [:error val]))
                  :error res))
        [:success []]
        eid-grouped))))