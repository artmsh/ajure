(ns ajure.specs
  (:require [clojure.spec.alpha :as s]
            [ajure.constants :as constants]))

(s/def ::db string?)
(s/def ::coll-name string?)
(s/def ::handle string?)
(s/def ::rev string?)

(s/def ::isSystem boolean?)
(s/def ::waitForSync boolean?)
(s/def ::silent boolean?)
(s/def ::returnNew boolean?)
(s/def ::doCompact boolean?)
(s/def ::isVolatile boolean?)
(s/def ::journalSize pos-int?)
(s/def ::replicationFactor pos-int?)
(s/def ::allowUserKeys boolean?)
(s/def :keyOptions/type #{"traditional" "autoincrement"})
(s/def ::increment pos-int?)
(s/def ::offset int?)
(s/def ::keyOptions (s/keys :opt-un [::allowUserKeys :keyOptions/type ::increment ::offset]))
(s/def :create-coll-options/type #{2 3})
(s/def ::shardKeys (s/coll-of vector? :kind string?))
(s/def ::numberOfShards pos-int?)
(s/def ::indexBuckets pos-int?)
(s/def ::doc map?)
(s/def ::docs (s/coll-of ::doc))
(s/def ::doc-or-docs (s/or :doc ::doc
                           :array ::docs))
(s/def :create-doc-options/collection string?)
(s/def ::create-doc-options (s/keys :opt-un [:create-doc-options/collection ::waitForSync ::returnNew ::silent]))
(s/def ::remove-doc-options (s/keys :opt-un [::waitForSync ::returnNew ::silent]))
(s/def ::create-coll-options
  (s/keys :req-un [::name]
          :opt-un [::journalSize ::replicationFactor ::keyOptions ::waitForSync ::doCompact ::isVolatile ::shardKeys
                   ::numberOfShards ::isSystem :create-coll-options/type ::indexBuckets]))
(s/def ::delete-coll-options (s/keys :opt-un [::isSystem]))

(s/def ::strategy constants/document-fetching-strategies)

(s/def :import-docs-options/type #{"documents" "auto" "list"})
(s/def :import-docs-options/collection string?)
(s/def ::fromPrefix string?)
(s/def ::toPrefix string?)
(s/def ::overwrite string?)
(s/def ::onDuplicate #{"error" "update" "replace" "ignore"})
(s/def ::complete string?)
(s/def ::details string?)
(s/def ::import-docs-options (s/keys :req-un [:import-docs-options/type :import-docs-options/collection]
                                     :opt-un [::fromPrefix ::toPrefix ::overwrite ::waitForSync ::onDuplicate ::complete ::details]))

(s/def ::collection string?)

(s/def ::keepNull boolean?)
(s/def ::mergeObjects boolean?)
(s/def ::ignoreRevs boolean?)
(s/def ::returnOld boolean?)

(s/def ::update-docs-options (s/keys :opt-un [::keepNull ::mergeObjects ::waitForSync ::ignoreRevs ::returnOld ::returnNew]))

(s/def ::query string?)
(s/def ::args (s/map-of string? any?))
(s/def ::query+args (s/keys :req-un [::query ::args]))
(s/def ::count int?)
(s/def ::batchSize pos-int?)
(s/def ::cache boolean?)
(s/def ::memoryLimit int?)
(s/def ::ttl int?)
(s/def ::options (s/map-of string? any?))
(s/def ::cursor-params (s/keys :opt-un [::count ::batchSize ::cache ::memoryLimit ::ttl ::options]))
(s/def ::reqs (s/coll-of record?))