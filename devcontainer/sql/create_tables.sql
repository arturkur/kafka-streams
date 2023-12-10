
CREATE TABLE supplier (
  id UUID PRIMARY KEY,
  name TEXT
);

CREATE TABLE sock (
  id UUID PRIMARY KEY,
  price BIGINT,
  type TEXT,
  supplier_id UUID NOT NULL
);

CREATE TABLE purchase (
  id UUID PRIMARY KEY,
  sock_id UUID NOT NULL,
  quantity BIGINT
);

CREATE TABLE sale (
  id UUID PRIMARY KEY,
  sock_id UUID,
  customer_name TEXT,
  price_per_pair DECIMAL,
  quantity BIGINT
);

CREATE TABLE result (
  id TEXT PRIMARY KEY,
  requirement TEXT,
  result TEXT
);
