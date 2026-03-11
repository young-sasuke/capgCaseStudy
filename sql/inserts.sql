-- ============================================================
-- Developer 6 - Sample Data (10+ records per table)
-- ============================================================

-- CUSTOMERS (12 rows)
INSERT INTO CUSTOMERS VALUES (1,  'Alice',   'Smith',    'alice@example.com',    'New York',    '2023-01-10');
INSERT INTO CUSTOMERS VALUES (2,  'Bob',     'Johnson',  'bob@example.com',      'Chicago',     '2023-02-15');
INSERT INTO CUSTOMERS VALUES (3,  'Carol',   'Williams', 'carol@example.com',    'Houston',     '2023-03-20');
INSERT INTO CUSTOMERS VALUES (4,  'David',   'Brown',    'david@example.com',    'Phoenix',     '2023-04-05');
INSERT INTO CUSTOMERS VALUES (5,  'Eve',     'Davis',    'eve@example.com',      'San Diego',   '2023-05-12');
INSERT INTO CUSTOMERS VALUES (6,  'Frank',   'Miller',   'frank@example.com',    'Dallas',      '2023-06-18');
INSERT INTO CUSTOMERS VALUES (7,  'Grace',   'Wilson',   'grace@example.com',    'San Jose',    '2023-07-22');
INSERT INTO CUSTOMERS VALUES (8,  'Henry',   'Moore',    'henry@example.com',    'Austin',      '2023-08-30');
INSERT INTO CUSTOMERS VALUES (9,  'Ivy',     'Taylor',   'ivy@example.com',      'Seattle',     '2023-09-14');
INSERT INTO CUSTOMERS VALUES (10, 'Jack',    'Anderson', 'jack@example.com',     'Denver',      '2023-10-01');
INSERT INTO CUSTOMERS VALUES (11, 'Karen',   'Thomas',   'karen@example.com',    'Boston',      '2023-11-05');
INSERT INTO CUSTOMERS VALUES (12, 'Leo',     'Jackson',  'leo@example.com',      'Atlanta',     '2023-12-20');

-- PRODUCTS (10 rows)
INSERT INTO PRODUCTS VALUES (1,  'Laptop',      'Electronics', 999.99, '2023-01-01');
INSERT INTO PRODUCTS VALUES (2,  'Mouse',       'Electronics',  29.99, '2023-01-01');
INSERT INTO PRODUCTS VALUES (3,  'Keyboard',    'Electronics',  59.99, '2023-01-01');
INSERT INTO PRODUCTS VALUES (4,  'Monitor',     'Electronics', 349.99, '2023-02-01');
INSERT INTO PRODUCTS VALUES (5,  'Headphones',  'Electronics', 149.99, '2023-02-01');
INSERT INTO PRODUCTS VALUES (6,  'Desk Chair',  'Furniture',   249.99, '2023-03-01');
INSERT INTO PRODUCTS VALUES (7,  'Standing Desk','Furniture',  499.99, '2023-03-01');
INSERT INTO PRODUCTS VALUES (8,  'Webcam',      'Electronics',  79.99, '2023-04-01');
INSERT INTO PRODUCTS VALUES (9,  'USB Hub',     'Accessories',  24.99, '2023-04-01');
INSERT INTO PRODUCTS VALUES (10, 'Cable Kit',   'Accessories',  14.99, '2023-05-01');

-- INVENTORY (10 rows)
INSERT INTO INVENTORY VALUES (1,  1,  'Warehouse-A', 50,  '2024-01-15');
INSERT INTO INVENTORY VALUES (2,  2,  'Warehouse-A', 200, '2024-01-15');
INSERT INTO INVENTORY VALUES (3,  3,  'Warehouse-A', 150, '2024-01-15');
INSERT INTO INVENTORY VALUES (4,  4,  'Warehouse-B', 30,  '2024-02-10');
INSERT INTO INVENTORY VALUES (5,  5,  'Warehouse-B', 80,  '2024-02-10');
INSERT INTO INVENTORY VALUES (6,  6,  'Warehouse-C', 40,  '2024-03-05');
INSERT INTO INVENTORY VALUES (7,  7,  'Warehouse-C', 20,  '2024-03-05');
INSERT INTO INVENTORY VALUES (8,  8,  'Warehouse-A', 100, '2024-04-01');
INSERT INTO INVENTORY VALUES (9,  9,  'Warehouse-B', 300, '2024-04-01');
INSERT INTO INVENTORY VALUES (10, 10, 'Warehouse-B', 500, '2024-05-01');

-- ORDERS (12 rows)
INSERT INTO ORDERS VALUES (1,  1,  '2024-01-20', 'Delivered',  1089.97);
INSERT INTO ORDERS VALUES (2,  2,  '2024-01-25', 'Delivered',   29.99);
INSERT INTO ORDERS VALUES (3,  3,  '2024-02-01', 'Shipped',    409.98);
INSERT INTO ORDERS VALUES (4,  1,  '2024-02-10', 'Delivered',  149.99);
INSERT INTO ORDERS VALUES (5,  4,  '2024-02-15', 'Pending',    749.98);
INSERT INTO ORDERS VALUES (6,  5,  '2024-03-01', 'Delivered',   89.98);
INSERT INTO ORDERS VALUES (7,  6,  '2024-03-10', 'Shipped',    249.99);
INSERT INTO ORDERS VALUES (8,  7,  '2024-03-20', 'Pending',    559.98);
INSERT INTO ORDERS VALUES (9,  2,  '2024-04-01', 'Delivered',   59.99);
INSERT INTO ORDERS VALUES (10, 8,  '2024-04-15', 'Pending',    999.99);
INSERT INTO ORDERS VALUES (11, 3,  '2024-05-01', 'Delivered',   14.99);
INSERT INTO ORDERS VALUES (12, 9,  '2024-05-10', 'Shipped',    104.98);

-- ORDER_ITEMS (15 rows)
INSERT INTO ORDER_ITEMS VALUES (1,  1,  1,  1, 999.99);
INSERT INTO ORDER_ITEMS VALUES (2,  1,  2,  1,  29.99);
INSERT INTO ORDER_ITEMS VALUES (3,  1,  3,  1,  59.99);
INSERT INTO ORDER_ITEMS VALUES (4,  2,  2,  1,  29.99);
INSERT INTO ORDER_ITEMS VALUES (5,  3,  4,  1, 349.99);
INSERT INTO ORDER_ITEMS VALUES (6,  3,  3,  1,  59.99);
INSERT INTO ORDER_ITEMS VALUES (7,  4,  5,  1, 149.99);
INSERT INTO ORDER_ITEMS VALUES (8,  5,  7,  1, 499.99);
INSERT INTO ORDER_ITEMS VALUES (9,  5,  6,  1, 249.99);
INSERT INTO ORDER_ITEMS VALUES (10, 6,  2,  1,  29.99);
INSERT INTO ORDER_ITEMS VALUES (11, 6,  3,  1,  59.99);
INSERT INTO ORDER_ITEMS VALUES (12, 7,  6,  1, 249.99);
INSERT INTO ORDER_ITEMS VALUES (13, 8,  7,  1, 499.99);
INSERT INTO ORDER_ITEMS VALUES (14, 8,  3,  1,  59.99);
INSERT INTO ORDER_ITEMS VALUES (15, 9,  3,  1,  59.99);
INSERT INTO ORDER_ITEMS VALUES (16, 10, 1,  1, 999.99);
INSERT INTO ORDER_ITEMS VALUES (17, 11, 10, 1,  14.99);
INSERT INTO ORDER_ITEMS VALUES (18, 12, 9,  1,  24.99);
INSERT INTO ORDER_ITEMS VALUES (19, 12, 8,  1,  79.99);

-- PAYMENTS (12 rows)
INSERT INTO PAYMENTS VALUES (1,  1,  '2024-01-20', 1089.97, 'Credit Card', 'Completed');
INSERT INTO PAYMENTS VALUES (2,  2,  '2024-01-25',   29.99, 'PayPal',      'Completed');
INSERT INTO PAYMENTS VALUES (3,  3,  '2024-02-02',  409.98, 'Credit Card', 'Completed');
INSERT INTO PAYMENTS VALUES (4,  4,  '2024-02-10',  149.99, 'Debit Card',  'Completed');
INSERT INTO PAYMENTS VALUES (5,  5,  NULL,           0.00,   NULL,          'Pending');
INSERT INTO PAYMENTS VALUES (6,  6,  '2024-03-01',   89.98, 'Credit Card', 'Completed');
INSERT INTO PAYMENTS VALUES (7,  7,  '2024-03-11',  249.99, 'PayPal',      'Completed');
INSERT INTO PAYMENTS VALUES (8,  8,  NULL,            0.00,  NULL,          'Pending');
INSERT INTO PAYMENTS VALUES (9,  9,  '2024-04-01',   59.99, 'Debit Card',  'Completed');
INSERT INTO PAYMENTS VALUES (10, 10, NULL,            0.00,  NULL,          'Pending');
INSERT INTO PAYMENTS VALUES (11, 11, '2024-05-01',   14.99, 'Credit Card', 'Completed');
INSERT INTO PAYMENTS VALUES (12, 12, '2024-05-10',  104.98, 'PayPal',      'Completed');
