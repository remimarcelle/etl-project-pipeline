-- Insert initial data into the branches table
INSERT INTO branches (name)
VALUES 
    ('Chesterfield'), 
    ('Nottingham');  

-- Insert initial data into the products table
INSERT INTO products (product_name, size, flavour, price)
VALUES 
    ('Latte', 'Regular', '', 2.50),            
    ('Tea', '', '', 1.50),                  
    ('Chocolate Cake', '', '', 3.00),         
    ('Carrot Cake', '', '', 2.80),              
    ('Cheese Cake', '', '', 4.00);             

-- Insert initial data into the transactions table
INSERT INTO transactions (branch_id, date_time, qty, price, payment_type)
VALUES 
    ((SELECT id FROM branches WHERE name='Chesterfield'), '2023-04-01 10:00:00', 2, 5.00, 'Cash'),
    ((SELECT id FROM branches WHERE name='Chesterfield'), '2023-04-01 11:00:00', 1, 2.50, 'Card');

-- Map transactions to products in the transaction_product table
INSERT INTO transaction_product (transaction_id, product_id)
VALUES 
    ((SELECT id FROM transactions WHERE date_time='2023-04-01 10:00:00'), (SELECT id FROM products WHERE product_name='Latte')), -- Linking transaction to latte
    ((SELECT id FROM transactions WHERE date_time='2023-04-01 11:00:00'), (SELECT id FROM products WHERE product_name='Tea'));   -- Linking transaction to tea
