-- Create the database if it does not exist
CREATE DATABASE IF NOT EXISTS cafe;

-- Switch to the cafe database
USE cafe;

-- Create the branches table
CREATE TABLE IF NOT EXISTS branches (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, 
    name VARCHAR(255) NOT NULL                
);

-- Create the transactions table
CREATE TABLE IF NOT EXISTS transactions (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, 
    branch_id INT NOT NULL,                  
    date_time DATETIME NOT NULL,              
    qty INT NOT NULL,                        
    price DECIMAL(10, 2),
    payment_type VARCHAR(50),                    
    FOREIGN KEY (branch_id) REFERENCES branches(id) 
);

-- Create the products table
CREATE TABLE products (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, 
    product_name VARCHAR(255) NOT NULL,        
    size VARCHAR(50),                         
    flavour VARCHAR(255),                      
    price DECIMAL(10, 2) NOT NULL                      
);

-- Create the transaction_product mapping table
CREATE TABLE transaction_product (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, 
    transaction_id INT NOT NULL,                
    product_id INT NOT NULL,                   
    FOREIGN KEY (transaction_id) REFERENCES transactions(id), 
    FOREIGN KEY (product_id) REFERENCES products(id)          
);
