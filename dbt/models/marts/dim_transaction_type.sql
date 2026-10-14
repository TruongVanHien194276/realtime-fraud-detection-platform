select *
from (
    values
        ('CASH_IN',  'Cash-in'),
        ('CASH_OUT', 'Cash-out'),
        ('DEBIT',    'Debit'),
        ('PAYMENT',  'Payment'),
        ('TRANSFER', 'Transfer')
) as types(transaction_type, display_name)