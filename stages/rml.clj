(ns rml
  (:require [clojure.string :as str]
            [insilica.ontology.biochem :as bc]
            [insilica.ontology.core :as ont :refer [clazz def-classes]]
            [tech.v3.dataset :as ds]
            [tmducken.duckdb :as duckdb]))

(def classes
  [(clazz
    "Tox21AggregatedRecord"
    nil
    "http://example.com/Tox21/AggregatedRecord"
    :resource-prefix "http://example.com/Tox21/AggregatedRecord/")
   (clazz
    "Tox21AssayOutcome"
    nil
    "http://example.com/Tox21/AssayOutcome"
    :property-type ont/XMLString)
   (clazz
    "Tox21ChannelOutcome"
    nil
    "http://example.com/Tox21/ChannelOutcome"
    :property-type ont/XMLString)
   (clazz
    "Tox21ConcArray"
    nil
    "http://example.com/Tox21/ConcArray"
    :property-type ont/XMLString)
   (clazz
    "Tox21CurveClass2"
    nil
    "http://example.com/Tox21/CurveClass2"
    :property-type ont/XMLDouble)
   (clazz
    "Tox21CurveRank"
    nil
    "http://example.com/Tox21/CurveRank"
    :property-type ont/XMLDouble)
   (clazz
    "Tox21DataArray"
    nil
    "http://example.com/Tox21/DataArray"
    :property-type ont/XMLString)
   (clazz
    "Tox21ID"
    nil
    "http://example.com/Tox21/ID"
    :property-type ont/XMLString)
   (clazz
    "Tox21HillCoeff"
    nil
    "http://example.com/Tox21/HillCoeff"
    :property-type ont/XMLDouble)
   (clazz
    "Tox21InfActivity"
    nil
    "http://example.com/Tox21/InfActivity"
    :property-type ont/XMLDouble)
   (clazz
    "Tox21PHill"
    nil
    "http://example.com/Tox21/PHill"
    :property-type ont/XMLDouble)
   (clazz
    "Tox21PurityRating"
    nil
    "http://example.com/Tox21/PurityRating"
    :property-type ont/XMLString)
   (clazz
    "Tox21R2"
    nil
    "http://example.com/Tox21/R2"
    :property-type ont/XMLDouble)
   (clazz
    "Tox21Record"
    nil
    "http://example.com/Tox21/Record"
    :resource-prefix "http://example.com/Tox21/Record/")
   (clazz
    "Tox21Reproducibility"
    nil
    "http://example.com/Tox21/Reproducibility"
    :property-type ont/XMLString)
   (clazz
    "Tox21SampleDataType"
    nil
    "http://example.com/Tox21/SampleDataType"
    :property-type ont/XMLString)
   (clazz
    "Tox21ZeroActivity"
    nil
    "http://example.com/Tox21/ZeroActivity"
    :property-type ont/XMLDouble)])

(def-classes classes)

(defn get-tox21 [conn]
  (duckdb/sql->dataset conn "select * from 'brick/tox21.parquet'"))

(defn get-tox21-aggregated [conn]
  (duckdb/sql->dataset conn "select * from 'brick/tox21_aggregated.parquet'"))

(defn get-tox21-lib [conn]
  (duckdb/sql->dataset conn "select * from 'brick/tox21lib.parquet'"))

(def re-conc #"CONC(\d+)")
(def re-data #"DATA(\d+)")

(def tox21-mapping
  {bc/hasAC50 "AC50"
   bc/hasCASNumber "CAS"
   bc/hasEfficacy "EFFICACY"
   bc/hasProtocolName "PROTOCOL_NAME"
   bc/hasPubchemCID #(-> % (get "PUBCHEM_CID") str parse-long)
   bc/hasPubchemSID #(-> % (get "PUBCHEM_SID") str parse-long)
   bc/hasSampleDataID "SAMPLE_DATA_ID"
   bc/hasSampleID "SAMPLE_ID"
   bc/hasSampleName "SAMPLE_NAME"
   bc/hasSMILES "SMILES"
   hasTox21ConcArray (fn [m]
                       (some->> m keys
                                (keep #(some-> (re-find re-conc %) second parse-long))
                                seq sort
                                (map #(get m (str "CONC" %)))
                                (str/join " ")))
   hasTox21CurveClass2 "CURVE_CLASS2"
   hasTox21DataArray (fn [m]
                       (some->> m keys
                                (keep #(some-> (re-find re-data %) second parse-long))
                                seq sort
                                (map #(get m (str "DATA" %)))
                                (str/join " ")))
   hasTox21HillCoeff "HILL_COEF"
   hasTox21ID "TOX21_ID"
   hasTox21InfActivity "INF_ACTIVITY"
   hasTox21PHill "P_HILL"
   hasTox21PurityRating "PURITY_RATING"
   hasTox21R2 "R2"
   hasTox21SampleDataType "SAMPLE_DATA_TYPE"
   hasTox21ZeroActivity "ZERO_ACTIVITY"})

(defn tox21-row->triples
  [{:as data :strs [SAMPLE_DATA_ID]}]
  (let [subj (ont/resource-iri Tox21Record SAMPLE_DATA_ID)]
    (concat
     [(ont/isA subj Tox21Record)]
     (ont/subj-mappings subj data tox21-mapping))))

(def tox21-aggregated-mapping
  {bc/hasAC50 "AC50"
   bc/hasCASNumber "CAS"
   bc/hasEfficacy "EFFICACY"
   bc/hasProtocolName "PROTOCOL_NAME"
   bc/hasPubchemCID #(-> % (get "PUBCHEM_CID") str parse-long)
   bc/hasPubchemSID #(-> % (get "PUBCHEM_SID") str parse-long)
   bc/hasSampleDataID "SAMPLE_DATA_ID"
   bc/hasSampleID "SAMPLE_ID"
   bc/hasSampleName "SAMPLE_NAME"
   bc/hasSMILES "SMILES"
   hasTox21AssayOutcome "ASSAY_OUTCOME"
   hasTox21ChannelOutcome "CHANNEL_OUTCOME"
   hasTox21CurveRank "CURVE_RANK"
   hasTox21ID "TOX21_ID"
   hasTox21PurityRating "PURITY_RATING"
   hasTox21Reproducibility "REPRODUCIBILITY"
   hasTox21SampleDataType "SAMPLE_DATA_TYPE"})

(defn tox21-aggregated-row->triples
  [{:as data :strs [SAMPLE_ID]}]
  (let [subj (ont/resource-iri Tox21AggregatedRecord SAMPLE_ID)]
    (concat
     [(ont/isA subj Tox21AggregatedRecord)]
     (ont/subj-mappings subj data tox21-aggregated-mapping))))

(defn -main [filename]
  (duckdb/initialize!)
  (let [db (duckdb/open-db)
        conn (duckdb/connect db)]
    (try
      (->> (mapcat tox21-row->triples (ds/rows (get-tox21 conn)))
           (concat (mapcat tox21-aggregated-row->triples (ds/rows (get-tox21-aggregated conn))))
           (ont/write-nt filename))
      (finally
        (duckdb/close-db db)))))

(comment
  (do
    (def filename "rdf/tox21.nt")
    (duckdb/initialize!)
    (def db (duckdb/open-db))
    (def conn (duckdb/connect db))
    (def tox21 (get-tox21 conn))
    (def tox21-aggregated (get-tox21-aggregated conn))
    (def tox21-lib (get-tox21-lib conn)))

  (def row1 (-> tox21 ds/rows first))
  (clojure.string/join " " (sort (keys row1)))
  (->> (tox21-row->triples row1) ont/nt-seq)
  (first (mapcat tox21-row->triples (ds/rows (get-tox21 conn))))

  (-> (-> tox21-aggregated ds/rows first)
      tox21-aggregated-row->triples ont/nt-seq)
  (-> tox21-lib ds/rows first)
  )
