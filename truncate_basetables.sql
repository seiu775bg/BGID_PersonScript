Stored Procedure Definition
CREATE OR REPLACE PROCEDURE pstg.truncate_basetables()
 LANGUAGE plpgsql
AS $$
BEGIN
delete from pstg.basehbt;
delete from pstg.basecontact;
END;
 $$