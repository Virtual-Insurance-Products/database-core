
(in-package :database-core)

;; Don't automatically require everything to be aliased
(defmethod requires-table-alias ((x t)) nil)

;; This generates SQL from something like datalog, which I quite like as a notation
;; It makes inner joins (a common case) very convenient)
(defun solve (constraints returning &key dont-error external-symbol-bindings alias-all-tables (next-table-mapping 0))
  (if (stringp constraints)
      constraints
      (let ( ;; hash of symbol->(table attribute)+
            (matched-symbols (make-hash-table :test #'eq))
            ;; attribute values is a list of (table attribute value)
            (attribute-values nil)
            (limit nil)
            (ordering nil)

            ;; we must rename all but the first instance of a given table. We can't rename the first because it would break sorting.
            (table-mappings (make-hash-table :test #'equal))
            ;; to get from the aliased name to the original name so that we can generate the table name list
            (reverse-table-mappings (make-hash-table))
            )
        
        ;; returned things
        (flet ((table-mapping (table)
                 (if (or (gethash table table-mappings)
                         alias-all-tables
                         (requires-table-alias table))
                     (let ((mapping (read-from-string (format nil "database-core::ttt-~A" next-table-mapping))))
                       (setf (gethash mapping reverse-table-mappings)
                             table)
                       (incf next-table-mapping)
                       mapping)
                     (progn (setf (gethash table reverse-table-mappings)
                                  table)
                            (setf (gethash table table-mappings)
                                  t)
                            table))))

          (let ((matches nil))

            (loop for a-constraint in constraints
               for table = (when (listp a-constraint) (first a-constraint))
               for attributes = (when (listp a-constraint) (cdr a-constraint))
               when (eq table :order-by)
               do (setf ordering attributes)

               when (eq table :limit)
               do (setf limit (first attributes))

               when (not table) do (push a-constraint matches)

               when (and table
                         (not (member table '(:limit :order-by :not :exists))))
               do (let ((table (table-mapping table)))
                    (loop for (key value) on attributes by #'cddr
                       do (cond ((and (symbolp value)
                                      (not (or (eq value t)
                                               (eq value nil))))
                                 ;; this is the case for doing inner table joins 
                                 (push (list table key) (gethash value matched-symbols)))

                                ;; yay - outer joins!
                                ((and (listp value)
                                      (= (length value) 1)
                                      (symbolp (first value)))
                                 (push (list table key :outer)
                                       (gethash (first value) matched-symbols)))

                                ((and (listp value)
                                      (= (length value) 2))
                                 (push (list table key value) attribute-values))

                                ;; this is the case for matching against simple values
                                (t (push (list table key value) attribute-values))))))

            ;; another phase: find symbols in comparators...
            (setf attribute-values
                  (loop for (table key value) in attribute-values
                     collect (list table key
                                   (if (and (listp value)
                                            (symbolp (second value))
                                            (not (eq (second value) t))
                                            (not (eq (second value) nil)))
                                       ;; this is a comparison with another item...
                                       (aif (or (gethash (second value) matched-symbols)
                                                (gethash (second value) external-symbol-bindings))
                                            (cons (first value) it)
                                            (error "Symbol ~A in ~A not defined in constraints"
                                                   (second value) value))
                                       value))))

            (loop for v being the hash-values of matched-symbols
                 using (hash-key symbol)
               for first = (first v)
               do (loop for a in (append (cdr v)
                                         (awhen (and external-symbol-bindings
                                                     (gethash symbol external-symbol-bindings))
                                           ;; I only need to match one instance. Everything else will be matched
                                           ;; either here or in the outer query
                                           (list (first it))))
                     do (push `(:eq1 ,(format nil "~A.~A~A" (sql-name (first first)) (sql-name (second first))
                                              (if (eq (third first) :outer) "(+)" ""))
                                     ,(format nil "~A.~A~A" (sql-name (first a)) (sql-name (second a))
                                              (if (eq (third a) :outer) "(+)" "")))
                              matches)))

            (loop for (table attribute value) in attribute-values
               ;; this 'eq' 'operator' is just use to differentiate between these and the above set of results
               do (if (and value (listp value))
                      ;; This allows arbitrary comparisons, rather than the default of equality
                      (push `(,(first value)
                               ,(format nil "~A.~A" (sql-name table) (sql-name attribute))
                               ,(second value))
                            matches)
                      (push `(:eq2 ,(format nil "~A.~A" (sql-name table) (sql-name attribute))
                                   ,value)
                            matches)))

            ;; Handle negation...
            (loop for c in constraints
               when (and (listp c)
                         (member (first c) '(:exists :not)))
               ;; now we have to make a subquery and pass in this value...
               do (push (let ((sub (solve (cdr c) nil :external-symbol-bindings matched-symbols :alias-all-tables t :next-table-mapping next-table-mapping)))
                          (setf (car sub) "*")
                          ;; append any additional constraints...
                          (s "~A EXISTS (~A)"
                             (if (eq (first c) :not) "NOT" "")
                             (sql-query sub)))
                        matches))
            
            
            (list (loop for k in returning
                     for v = (unless (stringp k)
                               (first (or (gethash k matched-symbols)
                                          (if dont-error
                                              nil
                                              (error (format nil "Returned symbol ~A not defined in constraints" k))))))
                     collect (if (stringp k)
                                 k
                                 (if (keywordp k)
                                     (format nil "count(~A.~A) AS ~A"
                                             (sql-name (first v))
                                             (sql-name (second v))
                                             (sql-name k))
                                     (if v
                                         (format nil "~A.~A AS ~A"
                                                 (sql-name (first v))
                                                 (sql-name (second v))
                                                 (sql-name k))
                                         (format nil "1 AS ~A" (sql-name k))))))

                  #+nil(loop for a-table in constraints
                          for table = (if (listp a-table) (first a-table))
                          when (and table
                                    (not (keywordp table)))
                          collect (sql-name table))
                  (loop for k being the hash-keys in reverse-table-mappings
                       using (hash-value v)
                       ;; !!! :new has special meaning here - it is the 'new' thingy used in stored procedures etc
                       when (and (eq k v) (not (eq k :new)))
                       collect (sql-name k)
                       unless (eq k v)
                       collect (format nil "~A AS ~A" (sql-name v) (sql-name k)))
                  matches ordering limit))))))

(defun sql-query (constraints)
  (if (stringp constraints)
      constraints             ; this means it's just an explicit query
      (concatenate 'string
                   (format nil "~%SELECT ~A ~%"
                           (if (stringp (first constraints))
                               (first constraints)
                               (string-list (first constraints) (format nil ",~%       "))))

                   ;; there don't actually have to be any tables either!
                   (if (second constraints)
                       (format nil "FROM   ~A ~%" (string-list (second constraints) (format nil ",~%       ")))
                       " ")
                   (if (third constraints)
                       (format nil "WHERE  ~A"
                               (string-list (loop for a-constraint in (third constraints)

                                               for type = (when (listp a-constraint) (first a-constraint))
                                               for left = (when (listp a-constraint) (second a-constraint))
                                               for right = (when (listp a-constraint) (third a-constraint))
                                                 
                                               collect (cond ((not type)
                                                              a-constraint)

                                                             ((eq type :eq1)
                                                              (format nil "~A=~A" left right))

                                                             ((eq type :eq2)
                                                              (format nil "~A=~A" left (sql-literal right)))

                                                             ((eq type '@@)
                                                              (s "to_tsvector('english', ~A) @@ to_tsquery('english', ~A)" left (sql-literal right)))
                                                             
                                                             #+nil((and (stringp type)
                                                                   (member (normalize-english type)
                                                                           '("LIKE" "ILIKE")
                                                                           :test #'equal))
                                                              (format nil "~A::text ~A ~A" left type (sql-literal right)))

                                                             ;; support for other comparisons...
                                                             (t
                                                              (format nil "~A ~A ~A" left type
                                                                      (if (listp right)
                                                                          ;; This means that it's a comparison with another column instead of a literal
                                                                          (s "~A.~A" (sql-name (first right)) (sql-name (second right)))
                                                                          (sql-literal right))))
                                                             )
                                               ;; *** FIX LITERAL SPLICING - there will be other data types
                                               )
                                            (format nil "~%  AND  ")))
                       "")
                   ;; *** Where does this key table come from?
                   ;; *** I should use sql-literal on the constraints
                   (when (fourth constraints)
                     (format nil "~%ORDER BY ~A"
                             (string-list #+nil(mapcar #'(lambda (x)
                                                           ;;
                                                           (format nil "~A.~A" *key-table* x))
                                                
                                                       (fourth constraints))
                                          (loop for (table key order) in (fourth constraints)
                                             collect (if (stringp table)
                                                         table
                                                         (format nil "~A.~A ~A"
                                                                 (sql-name table)
                                                                 (sql-name key)
                                                                 (symbol-name (or order :asc)))))
                                          ;; (fourth constraints)
                                          ", ")))
                   (when (fifth constraints)
                     (format nil "~%LIMIT ~A" (fifth constraints))))))


;; !!! Is it going to be ok that these symbols will be in the database-core package?
(defun all-columns (table &key (package :database-core))
  (mapcar #'(lambda (x)
              (intern (string-upcase (cl-ppcre:regex-replace-all "_" x "-"))
                      package))
          (pg-columns *database-connection*
                      (cl-ppcre:regex-replace-all "-"
                                                  (string-downcase (symbol-name table))
                                                  "_"))))

;; (all-columns 'broker)
;; (all-columns 'broker :package :vip)


(defmacro sql-loop (keys constraints &body body)
  ;; *** This idea, whilst a nice idea, does not work
  (let ((keys (mapcan #'copy-list
                      (mapcar #'(lambda (k)
                                  (if (keywordp k)
                                      (all-columns k)
                                      (list k)))
                              keys))))
    `(loop for ,keys in (dquery (sql-query (solve ,constraints ',keys)))
        ;; this is useful for drawing tables with alternating background colours
        for even/odd = "odd" then (if (equal even/odd "even") "odd" "even")
          ,@body)))

;; retrieve one row. Error if there are more than one
;; *** This is rather inefficiently implemented
(defmacro sql-get (key constraints &key multiple)
  (let ((i (gensym)))
    `(awhen (sql-loop (,key)
                ,constraints
              for ,i from 1
              when (> ,i 1) do (unless ,multiple (error (s "Too many rows attempting to get ~S of ~S" ,(list 'quote key) ,constraints)))
              collect ,key)
       (if ,multiple
           it (first it)))))

(defmacro sql-loop* (keys constraints &body body)
  ;; *** This now DOES work. It has to grab a new connection for its query in case subqueries are run. Bear that in mind
  (let ((keys (mapcan #'copy-list
                      (mapcar #'(lambda (k)
                                  (if (keywordp k)
                                      (all-columns k)
                                      (list k)))
                              keys)))
        (my-conn (gensym "CONN")))
    `(let ((,my-conn (new-database-connection)))
       (unwind-protect
            (cl-postgres:exec-query
             (database-core::connection ,my-conn)
             (sql-query (solve ,constraints ',keys))
     
             (cl-postgres:row-reader
                 (fields)
               (flet ((get-field (pos)
                        (cl-postgres:next-field (aref fields pos))))
                 (loop :while (cl-postgres:next-row)
                       ,@ (reduce #'append
                                  (loop for k in keys
                                        for i from 0
                                        collect `(:for ,k = (get-field ,i))))
                       for even/odd = "odd" then (if (equal even/odd "even") "odd" "even")
                       ,@body))))
         (pg-disconnect ,my-conn)))))

