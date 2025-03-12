-- Create the database if it does not exist
CREATE DATABASE IF NOT EXISTS cafe;

-- Switch to the cafe database
USE cafe;

-- Create the branches table
CREATE TABLE branches (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, 
    name VARCHAR(255) NOT NULL                
);

-- Create the products table
CREATE TABLE products (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, 
    product_name VARCHAR(250) NOT NULL,        
    size VARCHAR(50),                         
    flavour VARCHAR(50),                      
    price FLOAT NOT NULL                      
);

-- Create the transactions table
CREATE TABLE transactions (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, 
    branch_id INT NOT NULL,                  
    date_time DATETIME NOT NULL,              
    qty INT NOT NULL,                        
    price FLOAT NOT NULL,                    
    FOREIGN KEY (branch_id) REFERENCES branches(id) 
);

-- Create the transaction_product table
CREATE TABLE transaction_product (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, 
    transaction_id INT NOT NULL,                
    product_id INT NOT NULL,                   
    FOREIGN KEY (transaction_id) REFERENCES transactions(id), -
    FOREIGN KEY (product_id) REFERENCES products(id)          
);

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
INSERT INTO transactions (branch_id, date_time, qty, price)
VALUES 
    ((SELECT id FROM branches WHERE name='Chesterfield'), '2023-04-01 10:00:00', 2, 5.00),
    ((SELECT id FROM branches WHERE name='Chesterfield'), '2023-04-01 11:00:00', 1, 2.50); 

-- Map transactions to products in the transaction_product table
INSERT INTO transaction_product (transaction_id, product_id)
VALUES 
    ((SELECT id FROM transactions WHERE date_time='2023-04-01 10:00:00'), (SELECT id FROM products WHERE product_name='Latte')), -- Linking transaction to latte
    ((SELECT id FROM transactions WHERE date_time='2023-04-01 11:00:00'), (SELECT id FROM products WHERE product_name='Tea'));   -- Linking transaction to tea
