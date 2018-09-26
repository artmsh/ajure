(ns ajure.pull
  (:require [ajure.protocols :refer :all]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [cheshire.core :as json]
            [cheshire.generate :as json-gen]
            [clojure.walk :as walk])
  (:import (com.fasterxml.jackson.core JsonGenerator)))

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

(def counter (atom 0))

(defn gen-arango-var [prefix]
  (let [res (str prefix @counter)]
    (swap! counter inc)
    res))

(defn build-reverse-lookup [kw la return-only-id? doc-var]
  (let [[_ limit-amount] (if (nil? la) [nil nil] la)
        nsp (namespace kw)
        attr (name kw)
        header (str "FOR o in " nsp "\n")
        filter (str "FILTER o." (subs attr 1) " == " doc-var ".id\n")
        limit (when-not (nil? limit-amount) (str "LIMIT " limit-amount "\n"))
        return (str "RETURN " (if return-only-id? "o.id" "o") "\n")]
    (cond-> header
            filter (str filter)
            limit (str limit)
            return (str return))))

(defn kw->bind [kw] (symbol (gen-arango-var (str "$z" (name kw)))))

(defn build-lookup [kw la doc-var]
  (let [[_ limit-amount] (if (nil? la) [nil nil] la)
        b (kw->bind kw)]
    {:q (str "LET " b " = " (if (reverse-lookup-attr? kw)
                              (str "(" (build-reverse-lookup kw la true doc-var) ")")
                              (str doc-var "." (name kw))))
     :bind b}))

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
                           nsp (namespace kw)]
                       (case (first val)
                         :pattern
                         (let [b (kw->bind kw)]
                           {:q (str "LET " b " = (" (if (reverse-lookup-attr? kw)
                                                      (pattern->aql (second val) (build-reverse-lookup kw limit-amount false doc-var))
                                                      (pattern->aql (second val) (str doc-var "." (name kw))))
                                    ")")
                            :bind b})
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

(defrecord UnquotedString [s])
(json-gen/add-encoder UnquotedString
                      (fn [c ^JsonGenerator jsonGenerator]
                        (.writeRawValue jsonGenerator (str (:s c)))))

(defn find-binding [kw bind-names]
  (->UnquotedString
   (first (filter #(= (name kw) (remove-last-digits (subs (str %) 2))) bind-names))))

; * : (RETURN (MERGE_RECURSIVE document {id:_id, locationType:_locationType}))
(defn build-return-expr [tree doc-var bind-names]
  (let [attr-names (mapcat get-attr-names tree)
        returned-map (apply hash-map (mapcat #(vector (subs (str %) 1) (find-binding % bind-names)) attr-names))
        wildcard? (some #{'*} attr-names)]
    (if wildcard?
      (str "MERGE_RECURSIVE(" doc-var ", " (json/generate-string returned-map) ")")
      (json/generate-string returned-map))))

(defn pattern->aql [tree handles]
  (let [doc-var (gen-arango-var "document")
        header (str "FOR " doc-var " in " handles "\n")
        body (mapcat #(attr-spec->aql % doc-var) tree)]
        ;_ (prn (-> header
        ;           (str (str/join "\n "(map :q body)) "\n")
        ;           (str "RETURN " (build-return-expr tree doc-var (map :bind body)) "\n")))]
    (-> header
        (str (str/join "\n "(map :q body)) "\n")
        (str "RETURN " (build-return-expr tree doc-var (map :bind body)) "\n"))))

(defn build-query [tree handles]
  (let [str-handles (json/generate-string handles)
        aql (pattern->aql tree (str "DOCUMENT(" str-handles ")"))]
    {:query aql :args {}}))

(defrecord ArangodbPullApi [api]
  IPull
  (pull [this spec entity-ident]
    (let [pm (pull-many this spec [entity-ident])
          {:keys [error success]} pm]
      (if success
        {:success (first success)}
        {:error error})))
  (pull-many [this spec entity-idents]
    (let [eid-grouped (group-by first entity-idents)
          conformed-spec (s/conform ::pattern spec)]
      (reduce (fn [res [db entity-ids]]
                ; TODO hasMore eager loading
                (let [{:keys [error success]} (create-cursor api db (build-query conformed-spec (map second entity-ids)) {})]
                  (if success
                    (update res :success concat (walk/keywordize-keys (:result success)))
                    (reduced {:error error}))))
        {}
        eid-grouped))))