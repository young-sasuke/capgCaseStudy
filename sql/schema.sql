-- ============================================================
-- Developer 6 - SQL Schema for Order Processing System
-- ============================================================

DROP TABLE IF EXISTS PAYMENTS;
DROP TABLE IF EXISTS ORDER_ITEMS;
DROP TABLE IF EXISTS ORDERS;
DROP TABLE IF EXISTS INVENTORY;
DROP TABLE IF EXISTS PRODUCTS;
DROP TABLE IF EXISTS CUSTOMERS;

-- CUSTOMERS
CREATE TABLE CUSTOMERS (
    customer_id   INT PRIMARY KEY,
    first_name    VARCHAR(50)  NOT NULL,
    last_name     VARCHAR(50)  NOT NULL,
    email         VARCHAR(100) NOT NULL UNIQUE,
    city          VARCHAR(50),
    created_at    DATE
);

-- PRODUCTS
CREATE TABLE PRODUCTS (
    product_id    INT PRIMARY KEY,
    product_name  VARCHAR(100) NOT NULL,
    category      VARCHAR(50),
    price         DECIMAL(10,2) NOT NULL,
    created_at    DATE
);

-- INVENTORY
CREATE TABLE INVENTORY (
    inventory_id  INT PRIMARY KEY,
    product_id    INT NOT NULL,
    warehouse     VARCHAR(50),
    quantity      INT NOT NULL DEFAULT 0,
    last_updated  DATE,
    FOREIGN KEY (product_id) REFERENCES PRODUCTS(product_id)
);

-- ORDERS
CREATE TABLE ORDERS (
    order_id      INT PRIMARY KEY,
    customer_id   INT NOT NULL,
    order_date    DATE NOT NULL,
    status        VARCHAR(20) DEFAULT 'Pending',
    total_amount  DECIMAL(10,2),
    FOREIGN KEY (customer_id) REFERENCES CUSTOMERS(customer_id)
);

-- ORDER_ITEMS
CREATE TABLE ORDER_ITEMS (
    item_id       INT PRIMARY KEY,
    order_id      INT NOT NULL,
    product_id    INT NOT NULL,
    quantity      INT NOT NULL DEFAULT 1,
    unit_price    DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (order_id)   REFERENCES ORDERS(order_id),
    FOREIGN KEY (product_id) REFERENCES PRODUCTS(product_id)
);

-- PAYMENTS
CREATE TABLE PAYMENTS (
    payment_id    INT PRIMARY KEY,
    order_id      INT NOT NULL,
    payment_date  DATE,
    amount        DECIMAL(10,2) NOT NULL,
    method        VARCHAR(30),
    status        VARCHAR(20) DEFAULT 'Pending',
    FOREIGN KEY (order_id) REFERENCES ORDERS(order_id)
);
