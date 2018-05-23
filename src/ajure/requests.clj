(ns ajure.requests
  (:require [ajure.protocols :as proto]
            [ajure.handlers :refer :all]
            [cheshire.core :as json]
            [clojure.spec.alpha :as s]
            [clojure.edn :as edn]
            [clojure.string :as str]))

(defrecord Request [http-method url protocol-method output-spec responses http-params])

(defn create-database
  ([db]       (->Request :post "/_api/database" #'proto/create-database map?
                         {201 default-return-result
                          409 body-json-error}
                         {:form-params {:name db} :content-type :json}))
  ([db users] (->Request :post "/_api/database" #'proto/create-database map?
                         {201 default-return-result
                          409 body-json-error}
                         {:form-params {:name db :users users} :content-type :json})))

(defn get-collection-count [collection]
  (->Request :get (str "/_api/collection/" collection "/count") #'proto/get-collection-count map?
             {200 body-json-success
              400 body-json-error
              404 body-json-error}
             {}))

(defn get-document
  ([handle]              (->Request :get (str "/_api/document/" handle) #'proto/get-document map?
                                    {200 body-json-success
                                     404 body-json-error}
                                    nil))
  ([handle rev strategy] (->Request :get (str "/_api/document/" handle) #'proto/get-document map?
                                    {200 body-json-success
                                     404 body-json-error}
                                    {:headers
                                     (case strategy
                                       :if-none-match {"If-None-Match" rev}
                                       :if-match {"If-Match" rev}
                                       {})})))

(defn exist-document?
  ([handle]              (->Request :head (str "/_api/document/" handle) #'proto/exist-document?
                                    (s/or :s string? :n nil?)
                                    {200 {:success true} 404 {:success false}}
                                    nil))
  ([handle rev strategy] (->Request :head (str "/_api/document/" handle) #'proto/exist-document?
                                    (s/or :s string? :n nil?)
                                    {200 {:success true} 404 {:success false}}
                                    {:headers
                                     (case strategy
                                       :if-none-match {"If-None-Match" rev}
                                       :if-match {"If-Match" rev}
                                       {})})))

(defn get-api-version []
  (->Request :get "/_api/version" #'proto/get-api-version map? {200 body-json-success} nil))

(defn update-documents
  [collection documents]
  (->Request :patch (str "/_api/document/" collection) #'proto/update-documents map?
             {201 body-json-success
              202 body-json-success
              400 body-json-error
              404 body-json-error}
             {:body (json/generate-string documents)}))

(defn replace-document
  ([handle document replace-doc-options] (->Request :put (str "/_api/document/" handle) #'proto/replace-document map?
                                                    {201 body-json-success
                                                     202 body-json-success
                                                     400 body-json-error
                                                     404 body-json-error}
                                                    {:query-params replace-doc-options
                                                     :body (json/generate-string document)}))

  ([handle document rev replace-doc-options] (->Request :put (str "/_api/document/" handle) #'proto/replace-document map?
                                                        {201 body-json-success
                                                         202 body-json-success
                                                         400 body-json-error
                                                         404 body-json-error
                                                         412 body-json-error}
                                                        {:query-params replace-doc-options
                                                         :body (json/generate-string document)
                                                         :headers {"If-Match" rev}})))

(defn update-document
  ([handle document update-doc-options]
   (->Request :patch (str "/_api/document/" handle)
              #'proto/update-document map?
              {201 body-json-success
               202 body-json-success
               400 body-json-error
               404 body-json-error
               412 body-json-error}
              {:body (json/generate-string document)
               :query-params update-doc-options}))
  ([handle document rev update-doc-options]
   (->Request :patch (str "/_api/document/" handle)
              #'proto/update-document map?
              {201 body-json-success
               202 body-json-success
               400 body-json-error
               404 body-json-error
               412 body-json-error}
              {:body (json/generate-string document)
               :headers {"If-Match" rev}
               :query-params update-doc-options})))

(defn get-by-keys [collection keys]
  (->Request :put "/_api/simple/lookup-by-keys" #'proto/get-by-keys map?
             {200 (comp #(update % :success :documents) body-json-success)
              404 body-json-error
              405 body-json-error}
             {:body (json/generate-string {:keys keys :collection collection})}))

(defn get-by-example [collection doc get-by-example-options]
  (->Request :put "/_api/simple/by-example" #'proto/get-by-example map?
             {201 body-json-success
              400 body-json-error
              404 body-json-error}
             {:body (json/generate-string (merge {:collection collection
                                                  :example doc}
                                                 get-by-example-options))}))

(defn get-first-example [collection doc]
  (->Request :put "/_api/simple/first-example" #'proto/get-first-example map?
             {200 (comp #(update % :success :document) body-json-success)
              404 body-json-error}
             {:body (json/generate-string {:example doc
                                           :collection collection})}))

(defn get-all-documents
  ([collection]
   (->Request :put "/_api/simple/all" #'proto/get-all-documents map?
              {201 body-json-success
               400 body-json-error
               404 body-json-error}
              {:body (json/generate-string {:collection collection})}))
  ([collection skip limit]
   (->Request :put "/_api/simple/all" #'proto/get-all-documents map?
              {201 body-json-success
               400 body-json-error
               404 body-json-error}
              {:body (json/generate-string {:collection collection
                                            :skip skip
                                            :limit limit})})))

(defn get-next-cursor-batch [cursor-id]
  (->Request :put (str "/_api/cursor/" cursor-id) #'proto/get-next-cursor-batch map?
             {200 body-json-success
              400 body-json-error
              404 body-json-error}
             {}))

(defn get-all-document-keys
  ([collection]
   (get-all-document-keys collection nil))
  ([collection document-keys-type]
   (->Request :put "/_api/simple/all-keys" #'proto/get-all-document-keys map?
              {201 body-json-success
               404 body-json-error}
              {:body (json/generate-string (cond-> {:collection collection}
                                                   document-keys-type (merge {:type (name document-keys-type)})))})))

(defn get-replication-dump [collection replication-dump-options]
  (->Request :get "/_api/replication/dump" #'proto/get-replication-dump map?
             {200 body-json-replication-success
              204 body-json-replication-success
              400 body-json-error
              404 body-json-error
              405 body-json-error
              500 body-json-error}
             {:query-params (merge {:collection collection} replication-dump-options)}))

(defn get-collection-revision [collection]
  (->Request :get (format "/_api/collection/%s/revision" collection) #'proto/get-collection-revision map?
             {200 body-json-success
              400 body-json-error
              404 body-json-error}
             {}))