INSERT INTO inventory (sku, available, reserved)
VALUES
    ('SKU-1', 50, 0),
    ('SKU-2', 10, 0),
    ('SKU-3', 5, 0)
ON CONFLICT (sku) DO NOTHING;
