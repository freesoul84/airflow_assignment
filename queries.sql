INSERT INTO Customers (customer_id,name, email, country)
VALUES (1,'A','a@gmail.com','India'),
 (2,'G','g@gmail.com','India'),
 (3,'F','f@gmail.com','India'),
(4,'E','r@gmail.com','India'),
 (5,'D','d@gmail.com','India'),
 (6,'C','c@gmail.com','UK'),
 (7,'B','b@gmail.com','US');

 INSERT INTO categories (category_id, category_name) VALUES (1,'Books'),(2,'Electronics'),(3,'NA');

 delete from orders;
 INSERT INTO orders (order_id, customer_id, order_date, total_amount, status) 
 VALUES 
 (1,3,GETDATE(),5000,1),
 (2,2,GETDATE(),102400,1),
 (3,1,GETDATE(),30000,1);

 delete from products;
 INSERT INTO products (product_id, product_name, category_id) VALUES (1, 'ABC', 1), (2,'XYZ',2),(3,'PQR',1),(4,'LNM',1)


 INSERT INTO reviews (review_id, product_id, customer_id, rating, review_date) VALUES (1,1,3,4,getdate()),
  (2,1,1,1,getdate()),
   (3,2,2,5,getdate()),
    (4,3,3,4,getdate()),
	 (5,4,1,5,getdate()),
	  (6,1,2,2,getdate());

 INSERT INTO order_items (item_id, order_id, product_id, quantity, price) VALUES (1,1,1,2,2500), (2,2,4,12,200),(3,3,2,1,10000), (4,3,2,1,10000), (5,3,2,1,10000), (6,2,2,10,10000), (7,2,2,1,10000);
