-- Note: Table and column names have been anonymized for confidentiality

-- =============================================
-- Project: Card Transaction Monitoring
-- Description: Data extraction for credit card transactions
-- =============================================

SET vQVDname = 'fact_card_transactions';

TRACE _______ START DATA LOAD $(vQVDname) _______ ;

// =============================================
// PART 1: Base credit transactions
// =============================================
SQL
SELECT
    t.transaction_id,
    t.card_id,
    t.company_id,
    t.purchase_date,
    t.insert_timestamp,
    t.return_code,
    t.mcc_description,
    t.merchant_name,
    t.installment_number,
    t.transaction_amount,
    t.transaction_currency,
    t.settlement_currency,
    t.settlement_amount,
    t.converted_currency,
    t.converted_amount,
    t.mti_code,
    t.entry_mode,
    t.nsu,
    t.authorization_code,
    tp.wallet_provider
FROM fact_card_transactions t
LEFT JOIN dim_tokenized_purchase tp
    ON tp.purchase_id = t.purchase_id
    AND tp.card_id = t.card_id
    AND tp.wallet_provider IN ('APPLE_PAY', 'SAMSUNG_PAY')
WHERE t.purchase_date >= '$(vDataInicioCarga)'
  AND t.return_code <> 14

UNION ALL

// =============================================
// PART 2: Transaction events (+1 minute adjustment)
// =============================================
SELECT
    t.transaction_id,
    t.card_id,
    t.company_id,
    t.purchase_date,
    DATEADD(MINUTE, 1, e.event_timestamp) AS insert_timestamp,
    e.event_code AS return_code,
    t.mcc_description,
    t.merchant_name,
    t.installment_number,
    t.transaction_amount,
    t.transaction_currency,
    t.settlement_currency,
    t.settlement_amount,
    t.converted_currency,
    t.converted_amount,
    t.mti_code,
    t.entry_mode,
    t.nsu,
    t.authorization_code,
    tp.wallet_provider
FROM fact_card_transactions t
INNER JOIN fact_card_events e
    ON e.nsu = t.nsu
    AND e.authorization_code = t.authorization_code
LEFT JOIN dim_tokenized_purchase tp
    ON tp.purchase_id = t.purchase_id
    AND tp.card_id = t.card_id
    AND tp.wallet_provider IN ('APPLE_PAY', 'SAMSUNG_PAY')
WHERE e.event_timestamp >= '$(vDataInicioCarga)'
  AND e.event_code <> 14;

// =============================================
// LOAD INTO QVD (Qlik layer)
// =============================================
CONCATENATE

LOAD DISTINCT
    transaction_id,
    card_id,
    company_id,
    purchase_date,
    insert_timestamp,
    return_code,
    mcc_description,
    merchant_name,
    installment_number,
    transaction_amount
;
