INSERT INTO catalog.dataset_categories (
    category_slug,
    category_name,
    description,
    sort_order,
    is_active
)
VALUES
    (
        'health-medicine',
        'Health & Medicine',
        'Public datasets concerning health, medicine and population health.',
        10,
        TRUE
    ),
    (
        'crime-justice',
        'Crime & Justice',
        'Public datasets concerning crime, corrections and the justice system.',
        20,
        TRUE
    )
ON CONFLICT (category_slug) DO UPDATE
SET
    category_name = EXCLUDED.category_name,
    description = EXCLUDED.description,
    sort_order = EXCLUDED.sort_order,
    is_active = EXCLUDED.is_active,
    updated_at = now();