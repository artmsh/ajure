(ns ajure.core
  (:require [aleph.http :as http]
            [ajure.specs]
            [clojure.spec.alpha :as s]
            [expound.alpha :as expound]
            [ajure.protocols :refer :all]
            [ajure.handlers :refer :all]
            [ajure.helpers :refer :all]
            [cheshire.core :as json]
            [byte-streams :as bs]
            [clojure.string :as str]
            [ajure.requests :as reqs]))

(defn req
  ([{:keys [http-method url protocol-method output-spec responses http-params]} endpoint db args]
   (let [_url (if db (str "/_db/" db url) url)]
    (req protocol-method args output-spec http-method endpoint _url responses http-params)))
  ([sym args output-spec method url call] (req sym args output-spec method url call {200 default-return-result}))
  ([sym args output-spec method url call errors] (req sym args output-spec method url call errors nil))
  ([sym args output-spec method url call errors _params]
   ;(prn (str "CALL: " (-> method name str/upper-case) call))
   (let [arglists (map rest (:arglists (meta sym)))
         arglist (first (filter #(= (count %) (count args)) arglists))
         invalid-inputs (remove spec-valid? (zipmap arglist args))]
     (if (empty? invalid-inputs)
       (let [params (cond-> {:method method :url (str url call) :throw-exceptions? false}
                            (not (nil? _params)) (merge _params))
             rq @(http/request params)
             ;_ (clojure.pprint/pprint rq)
             ;_ (clojure.pprint/pprint (-> rq :body bs/to-string))
             [code body] (get-code-and-body-from-request rq)
             ;_ (prn [code body])
             handler (get errors code)]
         (handle-response handler code body (:headers rq) output-spec))
       {:error {:type :malformed-input
                :message (spec-explain (first invalid-inputs))}}))))

(defn as-http-content [req]
  (let [headers (get-in req [:http-params :headers])
        qs (get-in req [:http-params :query-params])
        body (get-in req [:http-params :body])
        url (cond-> (:url req) (not (nil? qs)) (str "?" (str/join "&" (map #(str (key %) "=" (val %)) qs))))
        method (-> req :http-method name str/upper-case)]
    (cond-> ""
            (not (nil? headers)) (str (str/join "\n" (map #(str (key %) ": " (val %)) headers)) "\n")
            true (str method " " url " HTTP/1.1")
            (not (nil? body)) (str "\n" body "\n"))))

(defrecord ArangodbApi [url]
  IArangodbApi
  (create-database [this db] (req (reqs/create-database db) url nil [db]))
  (create-database [this db users] (req (reqs/create-database db users) url nil [db users]))
  (get-current-database [this] (req #'get-current-database [] map? :get url "/_api/database/current"))


  (get-accessible-databases [this] (req #'get-accessible-databases [] map? :get url "/_api/database/user"))
  (get-all-databases [this] (req #'get-all-databases [] map? :get url "/_api/database"))
  (remove-database [this db] (req #'remove-database [db] map? :delete url (str "/_api/database/" db)
                                  {200 default-return-result
                                   400 (bad-request "Request is malformed")
                                   403 (forbidden "Request was not executed in the _system database")
                                   404 (not-found (str "Database '" db "' not exists"))}))

  (create-collection [this db create-coll-options]
    (req #'create-collection [db create-coll-options] map? :post url (str "/_db/" db "/_api/collection")
         {200 body-json-success}
         {:body (json/generate-string create-coll-options)}))

  (get-collection [this db coll-name]
    (req #'get-collection [db coll-name] map? :get url (str "/_db/" db "/_api/collection/" coll-name)
         {200 body-json-success
          404 (not-found (str "Collection '" coll-name "' is unknown"))}))
  (get-collection-properties [this db collection]
    (req #'get-collection-properties [db collection] map? :get url (str "/_db/" db "/_api/collection/" collection "/properties")
         {200 body-json-success
          404 (not-found (str "Collection '" collection "' is unknown"))
          400 (bad-request "Collection name is missing")}))
  (get-collection-count [this db collection]
    (req (reqs/get-collection-count collection) url db [db collection]))
  (get-collections [this db]
    (req #'get-collections [db] map? :get url (str "/_db/" db "/_api/collection")))
  (remove-collection [this db coll-name delete-coll-options]
    (req #'remove-collection [db coll-name] map? :delete url (str "/_db/" db "/_api/collection/" coll-name)
         {200 body-json-success
          400 (bad-request "Collection name is missing")
          404 (not-found (str "Collection '" coll-name "' is unknown"))}))
  (get-document [this db handle]
    (req (reqs/get-document handle) url db [db handle]))
  (get-document [this db handle rev strategy]
    ; TODO add error codes handlers
    (req #'get-document [db handle rev strategy] map? :get url (str "/_db/" db "/_api/document/" handle)
         {200 body-json-success
          404 (not-found (str "Document '" handle "' not found"))}
         {:headers
          (case strategy
            :if-none-match {"If-None-Match" rev}
            :if-match {"If-Match" rev}
            {})}))
  (exist-document? [this db handle]
    (req (reqs/exist-document? handle) url db [db handle]))
  (exist-document? [this db handle rev strategy]
    ; TODO add error codes handlers
    (req #'exist-document? [db handle rev strategy] nil? :head url (str "/_db/" db "/_api/document/" handle)
         {200 {:success true}
          404 {:success false}}
         {:headers
          (case strategy
            :if-none-match {"If-None-Match" rev}
            :if-match {"If-Match" rev}
            {})}))
  (create-document [this db coll-name document create-doc-options]
    (req #'create-document [db coll-name document create-doc-options] map? :post url (str "/_db/" db "/_api/document/" coll-name)
         {201 body-json-success
          202 body-json-success
          404 body-json-error
          400 body-json-error
          409 body-json-error}
         {:body (json/generate-string document)
          :query-params create-doc-options}))
  (remove-document [this db handle remove-doc-options]
    (req #'remove-document [db handle remove-doc-options] map? :delete url (str "/_db/" db "/_api/document/" handle)
         {200 body-json-success
          202 body-json-success
          404 body-json-error}))
  (remove-document [this db handle rev remove-doc-options]
    (req #'remove-document [db handle rev remove-doc-options] map? :delete url (str "/_db/" db "/_api/document/" handle)
         {200 body-json-success
          202 body-json-success
          404 body-json-error
          412 body-json-error}
         {:headers {"If-Match" rev}}))
  (import-documents [this db docs import-docs-options]
    (req #'import-documents [db docs import-docs-options] map? :post url (str "/_db/" db "/_api/import")
         {201 body-json-success
          400 body-json-error
          404 body-json-error
          409 body-json-error
          500 (server-error)}
         {:query-params import-docs-options
          :body (json/generate-string docs)}))
  (get-replication-dump [this db collection replication-dump-options]
    (req (reqs/get-replication-dump collection replication-dump-options) url db [db collection replication-dump-options]))
  (create-cursor [this db query+args cursor-params]
    (req #'create-cursor [db query+args cursor-params] map? :post url (str "/_db/" db "/_api/cursor")
         {201 body-json-success
          400 body-json-error}
         {:body (json/generate-string
                  (merge cursor-params {:query (:query query+args) :bindVars (:args query+args)}))}))
  (batch [this db reqs]
    (req #'batch [db reqs] map? :post url (str "/_db/" db "/_api/batch")
         {200 (partial batch-parse-result reqs)}
         {:multipart (map-indexed #(hash-map :name (str "req" %1)
                                             :content (as-http-content %2)
                                             :mime-type "application/x-arango-batchpart")
                                  reqs)}))
  (get-api-version [this] (req (reqs/get-api-version) url nil []))
  (update-documents [this db collection documents update-docs-options]
    (req (reqs/update-documents collection documents) url db [db collection documents update-docs-options]))
  (replace-document [this db handle document replace-doc-options]
    (req (reqs/replace-document handle document replace-doc-options) url db [db handle document replace-doc-options]))
  (replace-document [this db handle document rev replace-doc-options]
    (req (reqs/replace-document handle document rev replace-doc-options) url db [db handle document rev replace-doc-options]))
  (update-document [this db handle document update-doc-options]
    (req (reqs/update-document handle document update-doc-options) url db [db handle document update-doc-options]))
  (update-document [this db handle document rev update-doc-options]
    (req (reqs/update-document handle document rev update-doc-options) url db [db handle document rev update-doc-options]))
  (get-next-cursor-batch [this db cursor-id]
    (req (reqs/get-next-cursor-batch cursor-id) url db [db cursor-id]))
  IArangodbSimpleApi
  (get-all-documents [this db collection]
    (req (reqs/get-all-documents collection) url db [db collection]))
  (get-all-documents [this db collection skip limit]
    (req (reqs/get-all-documents collection skip limit) url db [db collection skip limit]))
  (get-all-document-keys [this db collection]
    (req (reqs/get-all-document-keys collection) url db [db collection]))
  (get-all-document-keys [this db collection document-keys-type]
    (req (reqs/get-all-document-keys collection document-keys-type) url db [db collection document-keys-type]))
  (get-by-keys [this db collection keys]
    (req (reqs/get-by-keys collection keys) url db [db collection keys]))
  (get-by-example [this db collection doc get-by-example-options]
    (req (reqs/get-by-example collection doc get-by-example-options) url db [db collection doc get-by-example-options]))
  (get-first-example [this db collection doc]
    (req (reqs/get-first-example collection doc) url db [db collection doc])))