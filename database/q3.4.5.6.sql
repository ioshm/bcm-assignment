CREATE OR REPLACE PACKAGE bcm_api AS
    FUNCTION sanitize_number(p_string VARCHAR2) RETURN INT;
    FUNCTION sanitize_date(p_string VARCHAR2) RETURN DATE;

    FUNCTION parse_order_id(p_order_ref VARCHAR2) RETURN INT;
    FUNCTION parse_order_line_id(p_order_ref VARCHAR2) RETURN INT;
    FUNCTION parse_invoice_id(p_invoice_ref VARCHAR2) RETURN INT;
    FUNCTION parse_primary_contact(p_contact VARCHAR2) RETURN INT;
    FUNCTION parse_secondary_contact(p_contact VARCHAR2) RETURN INT;

    TYPE order_invoice_rec IS RECORD (
        "Order Reference" NUMBER,
        "Order Period" VARCHAR2(64),
        "Supplier Name" VARCHAR2(64),
        "Order Total Amount" VARCHAR2(64),
        "Order Status" VARCHAR2(16),
        "Invoice Reference" NUMBER,
        "Invoice Total Amount" VARCHAR2(64),
        "Action" VARCHAR(32)
    );

    TYPE highest_order_total_amount_rec IS RECORD (
        "Order Reference" NUMBER,
        "Order Period" VARCHAR2(64),
        "Supplier Name" VARCHAR2(64),
        "Order Total Amount" VARCHAR2(64),
        "Order Status" VARCHAR2(16),
        "Invoice References" VARCHAR2(256)
    );

    TYPE number_of_orders_total_amount_rec IS RECORD (
        "Supplier Name" VARCHAR2(64),
        "Supplier Contact Name" VARCHAR(64),
        "Supplier Contact No. 1" VARCHAR(32),
        "Supplier Contact No. 2" VARCHAR(32),
        "Total Orders" NUMBER,
        "Order Total Amount" NUMBER
    );

    TYPE order_invoice_table IS TABLE OF order_invoice_rec;
    TYPE highest_order_total_amount_table IS TABLE OF highest_order_total_amount_rec;
    TYPE number_of_orders_total_amount_table IS TABLE OF number_of_orders_total_amount_rec;

    PROCEDURE migrate;
    FUNCTION order_invoice_details RETURN order_invoice_table PIPELINED;
    FUNCTION highest_order_total_amount RETURN highest_order_total_amount_table PIPELINED;
    FUNCTION number_of_orders_total_amount RETURN number_of_orders_total_amount_table PIPELINED;
END;
/

CREATE OR REPLACE PACKAGE BODY bcm_api AS
    -- Returns a number from an unsanitized number string input.
    FUNCTION sanitize_number(p_string VARCHAR2) RETURN INT IS
        v_number_cleaned VARCHAR2(16);
        v_number INT;
    BEGIN
        v_number_cleaned := REGEXP_REPLACE(p_string, '\s|,|\.');
        v_number_cleaned := REPLACE(v_number_cleaned, 'o', '0');
        v_number_cleaned := REPLACE(v_number_cleaned, 'I', '1');
        v_number_cleaned := REPLACE(v_number_cleaned, 'S', '5');
        v_number := TO_NUMBER(v_number_cleaned);
        RETURN v_number;
    END;

    -- Returns a date from an unsanitized date string input.
    --
    -- NOTE: Mostly to avoid taking chances with the database/session default date time format.
    FUNCTION sanitize_date(p_string VARCHAR2) RETURN DATE IS
        v_date DATE;
    BEGIN
        IF REGEXP_LIKE(p_string, '^[0-9]{2}-[A-Z]{3}-[0-9]{4}$') THEN
            v_date := TO_DATE(p_string, 'DD-MON-YYYY');
        ELSE
            v_date := TO_DATE(p_string, 'DD-MM-YYYY');
        END IF;
        RETURN v_date;
    END;

    -- Returns the id from "PO{id}"
    FUNCTION parse_order_id(p_order_ref VARCHAR2) RETURN INT IS
        v_number_match VARCHAR2(16);
        v_number INT;
    BEGIN
        v_number_match := SUBSTR(p_order_ref, 3, 3);
        v_number := TO_NUMBER(v_number_match);
        RETURN v_number;
    END;

    -- Returns the id from "POxxx-{id}".
    FUNCTION parse_order_line_id(p_order_ref VARCHAR2) RETURN INT IS
        v_number_match VARCHAR2(16);
        v_number INT;
    BEGIN
        v_number_match := SUBSTR(p_order_ref, 7, 1);

        -- If no order_line_id, then return null;
        IF v_number_match IS NULL THEN
            RETURN NULL;
        END IF;

        v_number := TO_NUMBER(v_number_match);
        RETURN v_number;
    END;

    -- Returns the id from "INV_POxxx.{id}".
    FUNCTION parse_invoice_id(p_invoice_ref VARCHAR2) RETURN INT IS
        v_number_match VARCHAR2(16);
        v_number INT;
    BEGIN
        v_number_match := SUBSTR(p_invoice_ref, 11);

        -- If no match, then return ID 1.
        IF v_number_match IS NULL THEN
            RETURN 1;
        END IF;

        v_number := TO_NUMBER(v_number_match) + 1;
        RETURN v_number;
    END;

    FUNCTION parse_primary_contact(p_contact VARCHAR2) RETURN INT IS
        v_position INT;
    BEGIN
        v_position := INSTR(p_contact, ',');

        IF v_position = 0 THEN
            RETURN bcm_api.sanitize_number(p_contact);
        END IF;

        RETURN bcm_api.sanitize_number(SUBSTR(p_contact, 1, v_position - 1));
    END;

    FUNCTION parse_secondary_contact(p_contact VARCHAR2) RETURN INT IS
        v_position INT;
    BEGIN
        v_position := INSTR(p_contact, ',');

        if v_position = 0 THEN
            RETURN NULL;
        END IF;

        RETURN bcm_api.sanitize_number(SUBSTR(p_contact, v_position + 1));
    END;

    -- Q.3
    PROCEDURE migrate IS
        TYPE t_order_mgt IS TABLE OF XXBCM_ORDER_MGT%ROWTYPE;

        v_order_mgt t_order_mgt;
        v_order_line t_order_mgt;
        v_invoice t_order_mgt;
    BEGIN
        -- TODO: Implement limit.
        -- TODO: Should probably perform the clean up and parsing step before filling tables.
        SELECT * BULK COLLECT INTO v_order_mgt FROM XXBCM_ORDER_MGT;

        SELECT * BULK COLLECT INTO v_order_line FROM XXBCM_ORDER_MGT
        WHERE parse_order_line_id(ORDER_REF) IS NOT NULL;

        SELECT * BULK COLLECT INTO v_invoice FROM XXBCM_ORDER_MGT
        WHERE INVOICE_REFERENCE IS NOT NULL;

        -- 1. Create the suppliers.
        FORALL i IN v_order_mgt.FIRST .. v_order_mgt.LAST
            MERGE INTO bcm_supplier s
            USING dual d
            ON (v_order_mgt(i).SUPPLIER_NAME = s.name)
            WHEN NOT MATCHED THEN
                INSERT (name, address, email, contact_name, contact_primary_number, contact_secondary_number)
                VALUES (
                    v_order_mgt(i).SUPPLIER_NAME,
                    v_order_mgt(i).SUPP_ADDRESS,
                    v_order_mgt(i).SUPP_EMAIL,
                    v_order_mgt(i).SUPP_CONTACT_NAME,
                    -- TODO: Sanitize this.
                    bcm_api.parse_primary_contact(v_order_mgt(i).SUPP_CONTACT_NUMBER),
                    bcm_api.parse_secondary_contact(v_order_mgt(i).SUPP_CONTACT_NUMBER)
                );

        -- 2. Create orders.
        FORALL i IN v_order_mgt.FIRST .. v_order_mgt.LAST
            MERGE INTO bcm_order o
            USING dual d
            ON (bcm_api.parse_order_id(v_order_mgt(i).ORDER_REF) = o.id)
            WHEN NOT MATCHED THEN
                INSERT (id, date_, status, supplier_id, total_amount)
                VALUES (
                    bcm_api.parse_order_id(v_order_mgt(i).ORDER_REF),
                    bcm_api.sanitize_date(v_order_mgt(i).ORDER_DATE),
                    v_order_mgt(i).ORDER_STATUS,
                    (SELECT id FROM bcm_supplier WHERE name = v_order_mgt(i).SUPPLIER_NAME),
                    bcm_api.sanitize_number(v_order_mgt(i).ORDER_TOTAL_AMOUNT)
                );

        -- 3. Create order lines.
        FORALL i IN v_order_line.FIRST .. v_order_line.LAST
            MERGE INTO bcm_order_line ol
            USING dual d
            ON ((bcm_api.parse_order_line_id(v_order_line(i).ORDER_REF) = ol.id AND
                bcm_api.parse_order_id(v_order_line(i).ORDER_REF) = ol.order_id))
            WHEN NOT MATCHED THEN
                INSERT (id, order_id, status, description, amount)
                VALUES (
                    bcm_api.parse_order_line_id(v_order_line(i).ORDER_REF),
                    bcm_api.parse_order_id(v_order_line(i).ORDER_REF),
                    v_order_line(i).ORDER_STATUS,
                    v_order_line(i).ORDER_DESCRIPTION,
                    bcm_api.sanitize_number(v_order_line(i).ORDER_LINE_AMOUNT)
                );

        -- 4. Create invoices.
        FORALL i IN v_invoice.FIRST .. v_invoice.LAST
            MERGE INTO bcm_invoice bi
            USING dual d
            ON (bcm_api.parse_order_id(v_invoice(i).ORDER_REF) = bi.order_id AND
                bcm_api.parse_invoice_id(v_invoice(i).INVOICE_REFERENCE) = bi.id)
            WHEN NOT MATCHED THEN
                INSERT (id, order_id, status, date_, description)
                VALUES (
                    bcm_api.parse_invoice_id(v_invoice(i).INVOICE_REFERENCE),
                    bcm_api.parse_order_id(v_invoice(i).ORDER_REF),
                    v_invoice(i).INVOICE_STATUS,
                    bcm_api.sanitize_date(v_invoice(i).INVOICE_DATE),
                    v_invoice(i).ORDER_DESCRIPTION
                );
        
        -- 5. Create invoice lines.
        FORALL i IN v_invoice.FIRST .. v_invoice.LAST
            INSERT INTO bcm_invoice_line (invoice_id, order_id, amount, hold_reason)
            VALUES (
                bcm_api.parse_invoice_id(v_invoice(i).INVOICE_REFERENCE),
                bcm_api.parse_order_id(v_invoice(i).ORDER_REF),
                bcm_api.sanitize_number(v_invoice(i).INVOICE_AMOUNT),
                v_invoice(i).INVOICE_HOLD_REASON
            );
    END;

    -- Q.4
    FUNCTION order_invoice_details RETURN order_invoice_table PIPELINED IS
    BEGIN
        FOR rec IN (
            SELECT
                o.id order_reference,
                TO_CHAR(o.date_, 'MON-YYYY') order_period,
                INITCAP(s.name) supplier_name,
                TO_CHAR(o.total_amount, '99,999,990.00') order_total_amount,
                o.status order_status,
                i.id invoice_reference,
                TO_CHAR(k.invoice_total, '99,999,990.00') invoice_total_amount,
                h.action
            FROM bcm_order o
                INNER JOIN bcm_supplier s ON o.supplier_id = s.id
                INNER JOIN bcm_invoice i ON o.id = i.order_id
                INNER JOIN (
                    SELECT
                        invoice_id,
                        order_id,
                        SUM(amount) invoice_total
                    FROM bcm_invoice_line
                    GROUP BY invoice_id, order_id
                ) k ON k.invoice_id = i.id AND k.order_id = i.order_id
                INNER JOIN (
                    SELECT
                        i1.order_id,
                        CASE 
                            WHEN COUNT(CASE WHEN i1.status = 'Paid' THEN 1 END) = COUNT(*) THEN 'OK'
                            WHEN COUNT(CASE WHEN i1.status = 'Pending' THEN 1 END) > 0 THEN 'To follow up'
                            WHEN COUNT(CASE WHEN i1.status IS NULL THEN 1 END) > 0 THEN 'To verify'
                        END action
                    FROM bcm_invoice i1
                        INNER JOIN bcm_order o1 ON i1.order_id = o1.id
                    GROUP BY i1.order_id
                ) h ON h.order_id = o.id
            ORDER BY o.id, i.id
        ) LOOP
            PIPE ROW (rec);
        END LOOP;
        RETURN;
    END;

    -- Q.5
    FUNCTION highest_order_total_amount RETURN highest_order_total_amount_table PIPELINED IS
    BEGIN
        FOR rec IN (
            SELECT
                id,
                TO_CHAR(date_, 'FMMonth DD, YYYY'), 
                name,
                TO_CHAR(total_amount, '99,999,990.00'),
                status,
                invoice_references
            FROM (
                SELECT 
                    o.id,
                    o.date_,
                    UPPER(s.name) name,
                    o.total_amount,
                    o.status,
                    k.invoice_references,
                    ROW_NUMBER() OVER (ORDER BY o.total_amount DESC) row_num
                FROM bcm_order o
                    INNER JOIN bcm_supplier s ON o.supplier_id = s.id
                    INNER JOIN bcm_invoice i ON o.id = i.order_id
                    INNER JOIN (
                        SELECT
                            o1.id order_id,
                            LISTAGG(CONCAT('INV_PO', LPAD(i1.order_id, 3, '0'), '.', i1.id), '|') as invoice_references
                        FROM bcm_order o1
                            INNER JOIN bcm_invoice i1 on o1.id = i1.order_id
                        GROUP BY o1.id) k ON o.id = k.order_id) t
            WHERE row_num = 2
        ) LOOP
            PIPE ROW (rec);
        END LOOP;
        RETURN;
    END;

    -- Q.6
    FUNCTION number_of_orders_total_amount RETURN number_of_orders_total_amount_table PIPELINED IS
    BEGIN
        FOR rec IN (
            SELECT
                s.name,
                s.contact_name,
                REGEXP_REPLACE(TO_CHAR(s.contact_primary_number, '99999999'), '(\d{4}|\d{3})(\d{4})', '\1-\2'),
                REGEXP_REPLACE(TO_CHAR(s.contact_secondary_number, '99999999'), '(\d{4}|\d{3})(\d{4})', '\1-\2'),
                k.total_orders,
                k.orders_total_amount
            FROM bcm_supplier s
                INNER JOIN (
                    SELECT
                        s1.id,
                        COUNT(*) total_orders,
                        SUM(o1.total_amount) orders_total_amount
                    FROM bcm_supplier s1
                        INNER JOIN bcm_order o1 ON s1.id = o1.supplier_id
                    WHERE o1.date_ BETWEEN '01-JAN-2022' AND '31-AUG-2022'
                    GROUP BY s1.id) k ON s.id = k.id
        ) LOOP
            PIPE ROW (rec);
        END LOOP;
        RETURN;
    END;
END;
/

EXECUTE bcm_api.migrate;
/

SELECT * FROM TABLE(bcm_api.order_invoice_details);
/

SELECT * FROM TABLE(bcm_api.highest_order_total_amount);
/

SELECT * FROM TABLE(bcm_api.number_of_orders_total_amount);
/
