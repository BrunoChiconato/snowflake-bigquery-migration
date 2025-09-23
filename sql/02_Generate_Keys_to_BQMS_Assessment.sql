ALTER USER BRUNOCHICONATO SET RSA_PUBLIC_KEY='<rsa_public_key>';

-- Both outputs should match:
-- Snowflake
DESC USER brunochiconato
  ->> SELECT SUBSTR(
        (SELECT "value" FROM $1
           WHERE "property" = 'RSA_PUBLIC_KEY_FP'),
        LEN('SHA256:') + 1) AS key;

-- Terminal
openssl rsa -pubin -in rsa_key.pub -outform DER | openssl dgst -sha256 -binary | openssl enc -base64
