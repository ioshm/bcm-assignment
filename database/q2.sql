DROP TABLE IF EXISTS bcm_invoice_line;
DROP TABLE IF EXISTS bcm_invoice;
DROP TABLE IF EXISTS bcm_order_line;
DROP TABLE IF EXISTS bcm_order;
DROP TABLE IF EXISTS bcm_supplier;

CREATE TABLE bcm_supplier(
    id                       INT GENERATED ALWAYS AS IDENTITY NOT NULL,
    name                     VARCHAR(64)    NOT NULL,
    address                  VARCHAR(256)   NOT NULL,
    email                    VARCHAR(256)   NOT NULL,

    contact_name             VARCHAR(64)    NOT NULL,
    contact_primary_number   INT            NOT NULL,
    contact_secondary_number INT                NULL,

    PRIMARY KEY(id)
);

CREATE TABLE bcm_order(
    id           INT            NOT NULL,
    date_        DATE           NOT NULL,
    status       VARCHAR(16)    NOT NULL CHECK(status IN ('Open', 'Closed')),
    supplier_id  INT            NOT NULL,
    total_amount DECIMAL(18, 2)     NULL,

    PRIMARY KEY(id),
    FOREIGN KEY(supplier_id) REFERENCES bcm_supplier(id)
);

CREATE TABLE bcm_order_line(
    id          INT             NOT NULL,
    order_id    INT             NOT NULL,
    status      VARCHAR(16)     NOT NULL CHECK(status IN ('Received', 'Cancelled')),
    description VARCHAR(64)     NOT NULL,
    amount      DECIMAL(18, 2)      NULL,

    PRIMARY KEY(id, order_id),
    FOREIGN KEY(order_id) REFERENCES bcm_order(id)
);

CREATE TABLE bcm_invoice(
    id          INT         NOT NULL,
    order_id    INT         NOT NULL,
    date_       DATE        NOT NULL,
    status      VARCHAR(16) NOT NULL CHECK(status IN ('Paid', 'Pending')),
    description VARCHAR(128),

    PRIMARY KEY(id, order_id),
    FOREIGN KEY(order_id) REFERENCES bcm_order(id)
);

CREATE TABLE bcm_invoice_line(
    invoice_id  INT             NOT NULL,
    order_id    INT             NOT NULL,
    amount      DECIMAL(18, 2)  NOT NULL,
    hold_reason VARCHAR(256)        NULL,

    FOREIGN KEY(invoice_id, order_id) REFERENCES bcm_invoice(id, order_id)
);
