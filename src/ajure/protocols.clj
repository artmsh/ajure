(ns ajure.protocols)

(defprotocol IArangodbApi
  (create-database [this db] [this db users])
  (get-current-database [this])
  (get-accessible-databases [this])
  (get-all-databases [this])
  (remove-database [this db])
  (create-collection [this db create-coll-options])
  (get-collection [this db coll-name])
  (get-collection-properties [this db coll-name])
  (get-collections [this db])
  (remove-collection [this db coll-name delete-coll-options])
  (get-document [this db handle] [this db handle rev strategy])
  (exist-document? [this db handle] [this db handle rev strategy])
  (create-document [this db coll-name doc-or-docs create-doc-options])
  ;(replace-document [this db handle document options] [this db handle document rev options])
  ;(replace-documents [this db collection documents options])
  ;(update-document [this db handle document options] [this db handle document rev options])
  (update-documents [this db collection documents update-docs-options])
  (remove-document [this db handle remove-doc-options] [this db handle rev remove-doc-options])
  ;(remove-documents [this db collection array options])
  ;(import-document-values [this db documents options])
  (import-documents [this db docs import-docs-options])
  ;(get-replication-inventory [this db options])
  ;(get-replication-dump [this db options]))

  (create-cursor [this db query+args cursor-params])
  (batch [this db reqs])
  (get-api-version [this]))

(defprotocol IArangodbSimpleApi
  (get-all-document-keys [this db collection] [this db collection type]))

(defprotocol IPull
  (pull [this spec entity-ident])
  (pull-many [this spec entity-idents]))