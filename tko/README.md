# TKO - test keryx output

tko is a simple query language for keryx streams.  It is used by the smoke tests for instance for specifying expected outputs.

## Conditions

tko supports a variety of condition types at the top level.  Each of these condition types have different expected query syntax.

### has_message - any message in the transaction matches

This would return true for any transaction that has a message for the users table that is missing fields data:
```json
'{"has_message":{"rel":"users", "missing_fields":true}}'
```

Fields of the message that can be queried are: type, db, ns, rel, ctid, prev_ctid. missing_fields is a boolean that will look for a messages with no field data if true, or with any field data if false. waits is a boolean that if set to true will match against the populate_wait field. fields_match allow you to check the specific fields of messages.

This will match any message with field foo == bar and goo == boo:
```json
'{"has_message":{"fields_match":{"foo":"bar", "goo":"boo"}}}'
```

### transaction_is - transaction has specific properties

This condition returns true if the transaction matches its query syntax.

To check a specific transaction id:
```json
'{"transaction_is":{"xid":78}}'
```

To check for big transactions:
```json
'{"transaction_is":{"big":true}}'
```

### transactions_that - stateful query that returns true once all conditions are matched across many transactions

This would return true once it saw transactions that were for for table users and a transaction that is big:
```json
'{"transactions_that":[{"has_message":{"rel":"users"}}, {"transaction_is":{"big":true}}]}'
```

*Note:* One transaction that was big and for users would count.

### not - inverts the condition

This will return true on any transaction that does not have a users message:
```json
'{"not":{"has_message":{"rel":"users"}}}'
```

### any_of - ors the conditions

This will return on any transaction that has a users message or a dogs message:
```json
'{"any_of":[{"has_message":{"rel":"users"}}, {"has_message":{"rel":"dogs"}}]}'
```

### all_of - ands the conditions

This will return on any transaction that hasb bot a users message and a dogs message:
```json
'{"any_of":[{"has_message":{"rel":"users"}}, {"has_message":{"rel":"dogs"}}]}'
```	

### always - always true

This is used when you want to return true as part of the query language.
```json
'{"always":{}}'
```
