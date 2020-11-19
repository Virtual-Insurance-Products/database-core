(in-package :database-core)

(defmacro with-pretend-database (&body forms)
  `(let ((*database-connection* (make-instance 'pretend-database)))
     ,@forms))

(defclass pretend-database ()
  ((next-id :initform 0)
   (assertions :accessor assertions :initform nil)))

(defmethod next-database-id ((x pretend-database))
  (incf (slot-value x 'next-id)))

(defclass pretend-database-statement ()
  ())

(defclass pretend-create-statement (pretend-database-statement)
  ((returning :initarg :returning :reader returning)
   (what :initarg :what :reader what)))

(defmethod db-assert-statement ((connection pretend-database) x &key returning)
  ;; !!! Check returning
  (make-instance 'pretend-create-statement
                 :returning returning
                 :what x))

;; (with-database (db-assert-statement *database-connection* '(broker :name "foo") :returning '(:id)))
;; (with-database (db-assert-statement *database-connection* '(quote) :returning '(:id :creation-date)))
;; (with-pretend-database (db-assert-statement *database-connection* '(broker :name "foo") :returning '(:id)))

(defmethod pg-exec/2 ((x pretend-database) (statement pretend-create-statement) &rest more)
  (declare (ignore more))
  (let ((result (list
                 (loop for value in (returning statement)
                       for name = (symbol-name value)
                       collect (cond ((equal name "ID")
                                      (next-database-id x))
                                     ;; this could scan for different names of date columns
                                     ;; it kind of needs knowledge of the schema to know what to return
                                     ;; we could somehow define that in the class, or in a subclass of pretend-database
                                     ((equal name "CREATION-DATE")
                                      (get-universal-time)))))))
    
    (push (list statement result) (assertions x))
    (list (cons :attributes (list
                             (loop for value in (returning statement)
                                for name = (symbol-name value)
                                collect (cond ((equal name "ID")
                                               '("id" 20 8))
                                              ((equal name "CREATION-DATE")
                                               ;; probably need a different tuple type here
                                               '("creation-date" 20 8))))))
          (cons :tuples result))))

(defmethod pg-exec ((x pretend-database) &rest args)
  (format *standard-output* "EXEC: ~A~%" args)
  (apply #'pg-exec/2 (cons x args)))


;; (with-pretend-database (make-instance 'v1:broker :name "Nortbert"))

;; how to handle setting slot values? IS there a method interface like #'db-assert-statement for those?
;; what about queries? It should be possible to do those too


;; !!! To make this work with other kinds of things (update slot value
;; !!! etc) I will need to factor out the SQL generation from the
;; !!! execution of updates. This happens to be done already for
;; !!! db-assert-statement, which is handy. Just need to do it more.
