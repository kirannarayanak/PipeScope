SELECT SUM(amount) AS revenue FROM {{ ref('stg_orders') }}
