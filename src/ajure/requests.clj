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
                         {201 default-return-result}
                         {:form-params {:name db} :content-type :json}))
  ([db users] (->Request :post "/_api/database" #'proto/create-database map?
                         {201 default-return-result}
                         {:form-params {:name db :users users} :content-type :json})))

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

(defn get-by-keys [collection keys]
  (->Request :put "/_api/simple/lookup-by-keys" #'proto/get-by-keys map?
             {200 (comp #(update % :success (fn [r] (get r "documents"))) body-json-success)
              404 body-json-error
              405 body-json-error}
             {:body (json/generate-string {:keys keys :collection collection})}))

(defn get-by-example [collection doc batchSize get-by-example-options]
  (prn (merge {:batchSize batchSize
               :collection collection
               :example doc}
              get-by-example-options))
  (->Request :put "/_api/simple/by-example" #'proto/get-by-example map?
             {201 body-json-success
              400 body-json-error
              404 body-json-error}
             {:body (json/generate-string (merge {:batchSize batchSize
                                                  :collection collection
                                                  :example doc}
                                                 get-by-example-options))}))

(defn get-next-cursor-batch [cursor-id]
  (->Request :put (str "/_api/cursor/" cursor-id) #'proto/get-next-cursor-batch map?
             {200 body-json-success
              400 body-json-error
              404 body-json-error}
             {}))

(defn body-json-replication-success [status body headers]
  {:success (if (str/blank? body)
              []
              (map json/parse-string (str/split-lines body)))
   :check-more    (edn/read-string (get headers "x-arango-replication-checkmore"))
   :last-included (edn/read-string (get headers "x-arango-replication-lastincluded"))})

(defn get-replication-dump [collection replication-dump-options]
  (->Request :get "/_api/replication/dump" #'proto/get-replication-dump map?
             {200 body-json-replication-success
              204 body-json-replication-success
              400 body-json-error
              404 body-json-error
              405 body-json-error
              500 body-json-error}
             {:query-params (merge {:collection collection} replication-dump-options)}))