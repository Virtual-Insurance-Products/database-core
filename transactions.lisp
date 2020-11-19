
(in-package :database-core)


;; should I implement a protocol to allow transactions to nest?
;; there are checkpoints in Pg which could be used for this...

(defparameter *in-transaction-p* nil)

;; The default implementation is that transactions don't nest, but this can be overriden
(defmethod do-nested-transaction ((connection t) (thunk function))
  (funcall thunk))

(defmethod do-database-transaction ((connection t) (thunk function))
  (funcall thunk))



;; if someone tries to do database-transaction w/o a connection we will create one first...
;; !!! I need to check that the methods all combine as we expect. I should probably just trace them and run
;; tests
(defmethod do-database-transaction ((connection (eql nil)) thunk)
  (with-database
    (do-database-transaction *database-connection* thunk)))


;; Taken from pg
(defmacro with-pg-transaction (con &body body)
  "Execute BODY forms in a BEGIN..END block.
If a PostgreSQL error occurs during execution of the forms, execute
a ROLLBACK command.
Large-object manipulations _must_ occur within a transaction, since
the large object descriptors are only valid within the context of a
transaction."
  (let ((success (gensym "SUCCESS")))
    `(let (,success)
       (unwind-protect
	    (prog2
                (exec-query (connection ,con) "BEGIN WORK")
		(progn ,@body)
	      (setf ,success t))
         (pg-exec ,con (if ,success "COMMIT WORK" "ROLLBACK WORK"))))))


;; this is defined as around to allow subclasses to do things INSIDE the transaction
;; if subclasses wish to do things OUTSIDE the transaction they can do so by defining
;; an :around method
(defmethod do-database-transaction :around ((connection database-connection-wrapper) (thunk function))
  (with-pg-transaction connection
    (call-next-method)))

;; NOT IMPLEMENTED: an :around for do-nested-transaction
;; This is intentional. I can implement it in a subclass

;; Arguably this should be called "with-database-transaction"
(defmacro database-transaction (&body forms)
  `(if *in-transaction-p*
       (do-nested-transaction *database-connection*
         (lambda ()
           ,@forms))
       (let ((*in-transaction-p* t))
         (with-database
           (do-database-transaction *database-connection*
             (lambda ()
               ,@forms))))))

(define-condition abort-transaction (error)
  ((value :initarg :value :reader value)))


;; If the transaction fails for any OTHER reason it will happen as normal...
(defmacro fail-database-transaction (&body forms)
  `(handler-case
       (database-transaction
         (let ((r (progn ,@forms)))
           (error 'abort-transaction :value r)))
     (abort-transaction (c)
       (value c))))
