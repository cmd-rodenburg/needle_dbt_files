select
	"id"					::BIGINT					AS PAYMENT_ID
	, "valueDate"			::DATE						AS PAYMENT_DATE
	, "amount"				::FLOAT						AS AMOUNT
	, "paymtPurpose"									AS DESCRIPTION
	, "checkAccount":"id"	::BIGINT					AS ACCOUNT_ID
	, CASE "status" 		:: INT
		WHEN 100 THEN 'Created'
		WHEN 200 THEN 'Linked'
		WHEN 300 THEN 'Private'
		WHEN 400 THEN 'Booked'
		ELSE "status" 		::TEXT
	END 												AS STATUS
	, "compareHash"
	, "payeePayerAcctNo"
	, "payeePayerBankCode"
	, "payeePayerName"
	, "entryType":"id"		::BIGINT					AS ENTRY_TYPE_ID
	, "create"				::TIMESTAMP WITH TIME ZONE	AS CREATE_TIMESTAMP
	, "update"				::TIMESTAMP WITH TIME ZONE	AS UPDATE_TIMESTAMP
from {{ source('sevdesk', 'payments')}}