ALTER TYPE linkedobjectroleschema ADD VALUE IF NOT EXISTS 'OwnerMailingaddress';
ALTER TYPE linkedobjectroleschema ADD VALUE IF NOT EXISTS 'MortgageMailingaddress';

INSERT INTO linkedobjectrole
    (lorid, lorschema_schemaid, title, description, createdts, deactivatedts, notes) VALUES
    (233, 'OwnerMailingaddress', 'owner mailing address', '', now(), null, null);

INSERT INTO linkedobjectrole
    (lorid, lorschema_schemaid, title, description, createdts, deactivatedts, notes) VALUES
    (234, 'MortgageMailingaddress', 'mortgage mailing address', '', now(), null, null);

ALTER TABLE mailingcitystatezip RENAME COLUMN source_sourceid To bobsource_sourceid;

