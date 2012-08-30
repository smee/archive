(ns archive 
  "Functions to help processing zip archive contents."
  (:use
    [clojure.java.io :only (as-file copy output-stream file)])
  (:import
    [java.io InputStream File ByteArrayInputStream ByteArrayOutputStream BufferedOutputStream FileOutputStream]
    [java.util.zip ZipInputStream ZipOutputStream ZipEntry ZipFile]))

(defn- to-byte-array [^InputStream x]
  (let [buffer (ByteArrayOutputStream.)]
    (copy x buffer)
    (.toByteArray buffer)))

(defn- delete-file-recursively 
  "FIXME doesn't delete directories reliably, need to sort them children before parents."
  [dir]
  (let [all-files (file-seq (as-file dir))
        files-only (filter #(.isFile %) all-files)
        dirs-only (filter #(.isDirectory %) all-files)]
    (doseq [f (concat files-only dirs-only)] (.delete f))))

(defn extract-entry
  "Extract file from zip, returns byte[]."
  [zipfile filename]
  (with-open [zf (ZipFile. zipfile)]
    (if-let [entry (.getEntry zf filename)]
      (to-byte-array (.getInputStream zf entry)))))

(defn extract-entry-to-file
  "Extract file from zip, copies contents into file in output directory."
  [zipfile filename output-dir]
  (with-open [zf (ZipFile. zipfile)]
    (if-let [entry (.getEntry zf filename)]
      (copy (.getInputStream zf entry) (file output-dir filename)))))

(defn- filter-entries [zf regex]
  (filter #(re-matches regex (.getName %))
            (enumeration-seq (.entries zf))))

(defn get-entries
  "Sequence of name of all entries of a zip archive matching a regular expression."
  ([zipfile] (get-entries zipfile #".*"))
  ([zipfile regex] 
    (with-open [zf (ZipFile. zipfile)]
      (doall (map (fn [^ZipEntry ze] (.getName ze)) (filter-entries zf regex))))))


(defn- process-entries-internal
  "Run function for every entry (two parameters: entry name and contents as byte-array), 
returns sequence of results (not lazy), sequential."
  [map-fn zipfile func regex]
  (with-open [zf (ZipFile. zipfile)]
    (doall 
      (map-fn #(func (.getName %) (to-byte-array (.getInputStream zf %))) (filter-entries zf regex)))))  

(defn process-entries
  "Run function for every entry (two parameters: entry name and contents as byte-array), 
returns sequence of results (not lazy), runs sequentially via map."
  ([zipfile func] (process-entries zipfile func #".*"))
  ([zipfile func regex] (process-entries-internal map zipfile func regex)))

(defn pprocess-entries
  "Run function for every entry (two parameters: entry name and contents as byte-array), 
returns sequence of results (not lazy), runs parallel via pmap."
  ([zipfile func] (pprocess-entries zipfile func #".*"))
  ([zipfile func regex] (process-entries-internal pmap zipfile func regex)))  

(defn broken? 
  "Is the given file a broken zip archive?"
  [file]
  (try
    (dorun (get-entries (as-file file)))
    false
    (catch Exception e (.printStackTrace e) true)))

(defn- unix-path [path]
  (.replaceAll path "\\\\" "/"))

(defn- trim-leading-str [s to-trim]
  (subs s  (.length to-trim)))

(defn copy-to-zip 
  "Copy all files under root-dir into a zip archive located at zip-file. Uses pathes relative to root-dir."
  ([zip-file root-dir] (copy-to-zip zip-file root-dir false))
  ([zip-file root-dir move?]
    (let [root (str (unix-path root-dir) \/)
          files (seq (->> root-dir as-file file-seq (filter (memfn isFile))))]
      (when files
        (with-open [zip-os (-> zip-file
                             (FileOutputStream.)
                             (BufferedOutputStream.)
                             (ZipOutputStream.))]
          (doseq [file files]
            (let [path (unix-path (trim-leading-str (str file) root))]
              (.putNextEntry zip-os (doto (ZipEntry. path)
                                      (.setTime (.lastModified file))))
              (copy file zip-os)))
          (when move?
            (delete-file-recursively root-dir)))))))

(defn write-into-zip [outfile name-in-zip in-stream]
  (with-open [zip-os (-> outfile
                       (FileOutputStream.)
                       (BufferedOutputStream.)
                       (ZipOutputStream.))]
    (.putNextEntry zip-os (ZipEntry. name-in-zip))
    (copy in-stream zip-os)))

(defn copy-into-zip [outfile names-n-streams]
  (with-open [zip-os (-> outfile
                       (FileOutputStream.)
                       (BufferedOutputStream.)
                       (ZipOutputStream.))]
    (doseq [[name stream] names-n-streams]
      (.putNextEntry zip-os (ZipEntry. (unix-path name)))
      (copy stream zip-os))))

(defn merge-zip-files [zip1 zip2 outfile]
  (with-open [zf1 (ZipFile. zip1)
              zf2 (ZipFile. zip2)
              all-entries (concat (map (juxt (memfn getName) #(.getInputStream zf1 %)) (enumeration-seq (.entries zf1)))
                                  (map (juxt (memfn getName) #(.getInputStream zf2 %)) (enumeration-seq (.entries zf2))))]
    (copy-into-zip outfile all-entries)))

(defn name-only [zipentry]
  (let [n (unix-path (.getName zipentry))] 
    (subs n (.lastIndexOf n (int \/)))))


