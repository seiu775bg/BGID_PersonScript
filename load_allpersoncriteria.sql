Stored Procedure Definition
CREATE OR REPLACE PROCEDURE pstg.load_allpersoncriteria()
 LANGUAGE plpgsql
AS $$
BEGIN

/*First Pass Logic:
Matching criteria - (kn_key_name = substring(c.dshs_provider_id, 1, 7))
and records not present in person table*/

update pstg.person
set kn_key_name = b.kn_key_name,
prt_first = b.prt_first,
prt_last = b.prt_last,
prt_ss_nbr_last4 = b.prt_ss_nbr_last4,
prt_birth_date = b.prt_birth_date,
prt_employer_code = b.prt_employer_code,
prt_telephone_nbr = b.prt_telephone_nbr,
prt_zip_code = b.prt_zip_code,
prt_ss_nbr = b.prt_ss_nbr,
lastmodified = sysdate,
zen_age = b.zen_age,
sex = b.sex,
prt_address1 = b.prt_address1,
prt_address2 = b.prt_address2
from pstg.person t
join pstg.basehbt b
on left(t.dshs_provider_id, 7) = b.kn_key_name
and pass_criteria like '1%';

update pstg.person
set contactid = c.contactid,
student_id = c.student_id,
firstname = c.firstname,
lastname = c.lastname,
social_security_number_last4 = c.social_security_number_last4,
birthdate = c.birthdate,
phone = c.phone, mobilephone = c.mobilephone,
homephone = c.homephone,
dshs_provider_id = c.dshs_provider_id,
mailingpostalcode = c.mailingpostalcode,
lastmodified = sysdate,
birthdate_age = c.birthdate_age,
address = c.address
from pstg.person t
join pstg.basecontact c
on (t.kn_key_name = substring(c.dshs_provider_id, 1, 7))
and pass_criteria like '1%';




insert into pstg.person(contactid,
                               student_id,
                               kn_key_name,
                               firstname,
                               lastname,
                               prt_ss_nbr_last4,
                               social_security_number_last4,
                               prt_birth_date,
                               birthdate,
                               prt_employer_code,
                               prt_telephone_nbr,
                               phone,
                               mobilephone,
                               homephone,
                               prt_zip_code,
                               dshs_provider_id,
                               prt_ss_nbr,
                               mailingpostalcode,
                               prt_first,
                               prt_last,
                               zen_age,
                               birthdate_age,
                               sex,
                               address,
                               prt_address1,
                               prt_address2,
                               pass_criteria)
                               (SELECT DISTINCT c.contactid,
                                                c.student_id,
                                                kn_key_name,
                                                firstname,
                                                lastname,
                                                prt_ss_nbr_last4,
                                                social_security_number_last4,
                                                prt_birth_date,
                                                c.birthdate,
                                                prt_employer_code,
                                                prt_telephone_nbr,
                                                c.phone,
                                                c.mobilephone,
                                                c.homephone,
                                                prt_zip_code,
                                                substring(c.dshs_provider_id, 1, 7) AS dshs_provider_id,
                                                cast(prt_ss_nbr as varchar(20)),
                                                mailingpostalcode,
                                                prt_first,
                                                prt_last,
                                                zen_age,
                                                birthdate_age,
                                                sex,
                                                address,
                                                prt_address1,
                                                prt_address2,
                                                '1'
from pstg.basecontact c join pstg.basehbt h on (kn_key_name = substring(c.dshs_provider_id, 1, 7))
/* For records not present in Person Table*/
where not exists(select 1 from pstg.person b where b.kn_key_name = h.kn_key_name)
and kn_key_name <> '0'
and kn_key_name <> ''
and dshs_provider_id <> '0'
and dshs_provider_id <> ''
);

/*
 Secondpass logic:
Matching criteria- (firstname = prt_first and lastname = prt_last)
and (length(prt_ss_nbr) >= 8)
and (prt_ss_nbr_last4 = social_security_number_last4)
and (date(birthdate) = date(prt_birth_date))
and (prt_zip_code = mailingpostalcode)
and (prt_telephone_nbr = phone or prt_telephone_nbr = mobilephone or prt_telephone_nbr = homephone) and records not present in person table
 */



update pstg.person
set kn_key_name = b.kn_key_name,
    prt_first = b.prt_first,
    prt_last = b.prt_last,
    prt_ss_nbr_last4 = b.prt_ss_nbr_last4,
    prt_birth_date = b.prt_birth_date,
    prt_employer_code = b.prt_employer_code,
    prt_telephone_nbr = b.prt_telephone_nbr,
    prt_zip_code = b.prt_zip_code,
    prt_ss_nbr = b.prt_ss_nbr,
    lastmodified = sysdate,
    zen_age = b.zen_age,
    sex = b.sex,
    prt_address1 = b.prt_address1,
    prt_address2 = b.prt_address2
from pstg.person t
join pstg.basehbt b
on (b.prt_first = t.firstname
and b.prt_last = t.lastname
and b.prt_ss_nbr_last4 = t.social_security_number_last4
and (date(b.prt_birth_date) = date(t.birthdate))
and b.prt_zip_code = t.mailingpostalcode
and (b.prt_telephone_nbr = t.phone
or b.prt_telephone_nbr = t.mobilephone
or b.prt_telephone_nbr = t.homephone
)
and pass_criteria like '2%'
);

update pstg.person
set contactid = c.contactid,
    student_id = c.student_id,
    firstname = c.firstname,
    lastname = c.lastname,
    social_security_number_last4 = c.social_security_number_last4,
    birthdate = c.birthdate,
    phone = c.phone,
    mobilephone = c.mobilephone,
    homephone = c.homephone,
    dshs_provider_id = c.dshs_provider_id,
    mailingpostalcode = c.mailingpostalcode,
    lastmodified = sysdate,
    birthdate_age = c.birthdate_age,
    address = c.address
from pstg.person b
join pstg.basecontact c
on (b.prt_first = c.firstname
and b.prt_last = c.lastname
and b.prt_ss_nbr_last4 = c.social_security_number_last4
and b.prt_birth_date = c.birthdate
and b.prt_zip_code = c.mailingpostalcode
and (b.prt_telephone_nbr = c.phone
or b.prt_telephone_nbr = c.mobilephone
or b.prt_telephone_nbr = c.homephone)
and pass_criteria like '2%'
);



-- Second Criteria
insert into pstg.person(
                               contactid,
                               student_id,
                               kn_key_name,
                               firstname,
                               lastname,
                               prt_ss_nbr_last4,
                               social_security_number_last4,
                               prt_birth_date,
                               birthdate,
                               prt_employer_code,
                               prt_telephone_nbr,
                               phone,
                               mobilephone,
                               homephone,
                               prt_zip_code,
                               dshs_provider_id,
                               prt_ss_nbr,
                               mailingpostalcode,
                               prt_first,
                               prt_last,
                               zen_age,
                               birthdate_age,
                               sex,
                               address,
                               prt_address1,
                               prt_address2,
                               pass_criteria)
                               (SELECT contactid,
                                       student_id,
                                       kn_key_name,
                                       firstname,
                                       lastname,
                                       prt_ss_nbr_last4,
                                       social_security_number_last4,
                                       prt_birth_date,
                                       birthdate,
                                       prt_employer_code,
                                       prt_telephone_nbr,
                                       phone,
                                       mobilephone,
                                       homephone,
                                       prt_zip_code,
                                       dshs_provider_id,
                                       prt_ss_nbr,
                                       mailingpostalcode,
                                       prt_first,
                                       prt_last,
                                       zen_age,
                                       birthdate_age,
                                       sex, address,
                                       prt_address1,
                                       prt_address2,
                                       '2'
                                        FROM
(SELECT contactid, student_id, kn_key_name, firstname, lastname, prt_ss_nbr_last4,
social_security_number_last4, prt_birth_date, birthdate, prt_employer_code, prt_telephone_nbr,
phone, mobilephone, homephone, prt_zip_code, dshs_provider_id, prt_ss_nbr, mailingpostalcode, prt_first, prt_last, zen_age, birthdate_age,sex, address, prt_address1, prt_address2, '2',
ROW_NUMBER() OVER( PARTITION BY firstname,lastname, prt_ss_nbr, prt_birth_date, prt_zip_code, prt_telephone_nbr
ORDER BY contactid ) AS row_num
from pstg.basehbt h join pstg.basecontact f
/*logic as per second criteria*/
on (firstname = prt_first and lastname = prt_last)
and (length(prt_ss_nbr) >= 8)
and (prt_ss_nbr_last4 = social_security_number_last4)
and (date(birthdate) = date(prt_birth_date))
and (prt_zip_code = mailingpostalcode)
and (prt_telephone_nbr = phone
or prt_telephone_nbr = mobilephone
or prt_telephone_nbr = homephone)
/*Below logics nowhere mentioned in criteria */
and mailingpostalcode != '0'
and mailingpostalcode != ''
and prt_ss_nbr <> ''
and prt_ss_nbr <> '0'
and prt_telephone_nbr <> ''
and prt_telephone_nbr <> '0'
and prt_birth_date is not null
--Get missing contacts which are not present in Person Table
and not exists (select 1 from pstg.person b where b.lastname = f.lastname and b.firstname = f.firstname and (b.prt_ss_nbr_last4 = h.prt_ss_nbr_last4)
and (date(b.birthdate) = date(f.birthdate))
and (b.prt_zip_code = h.prt_zip_code)
and (b.prt_telephone_nbr = h.prt_telephone_nbr))
) t
where t.row_num < 2);


/*
Thirdpass logic:
Matching criteria- (firstname = prt_first and lastname = prt_last)
and (length(prt_ss_nbr) >= 8)
and (prt_ss_nbr_last4 = social_security_number_last4)
and (date(birthdate) = date(prt_birth_date))
and (prt_zip_code = mailingpostalcode)
and records not present in person table
 */



update pstg.person
set kn_key_name = b.kn_key_name,
    prt_first = b.prt_first,
    prt_last = b.prt_last,
    prt_ss_nbr_last4 = b.prt_ss_nbr_last4,
    prt_birth_date = b.prt_birth_date,
    prt_employer_code = b.prt_employer_code,
    prt_telephone_nbr = b.prt_telephone_nbr,
    prt_zip_code = b.prt_zip_code,
    prt_ss_nbr = b.prt_ss_nbr,
    lastmodified = sysdate,
    zen_age = b.zen_age,
    sex = b.sex,
    prt_address1 = b.prt_address1,
    prt_address2 = b.prt_address2
from pstg.person t
join pstg.basehbt b
    /* Logic as per third pass*/
on (b.prt_first = t.firstname
and b.prt_last = t.lastname
and b.prt_ss_nbr_last4 = t.social_security_number_last4
and b.prt_birth_date = t.birthdate
and b.prt_zip_code = t.mailingpostalcode
)
and pass_criteria like '3%';


update pstg.person
set
    contactid = c.contactid,
    student_id = c.student_id,
    firstname = c.firstname,
    lastname = c.lastname,
    social_security_number_last4 = c.social_security_number_last4,
    birthdate = c.birthdate,
    phone = c.phone,
    mobilephone = c.mobilephone,
    homephone = c.homephone,
    dshs_provider_id = c.dshs_provider_id,
    mailingpostalcode = c.mailingpostalcode,
    lastmodified = sysdate,
    birthdate_age = c.birthdate_age,
    address = c.address
from pstg.person b
join pstg.basecontact c
    /* Logic as per third pass*/
on (b.prt_first = c.firstname
and b.prt_last = c.lastname
and b.prt_ss_nbr_last4 = c.social_security_number_last4
and b.prt_birth_date = c.birthdate
and b.prt_zip_code = c.mailingpostalcode
)
and pass_criteria like '3%';



	-- Third Criteria 
insert into pstg.person(
                               contactid,
                               student_id,
                               kn_key_name,
                               firstname,
                               lastname,
                               prt_ss_nbr_last4,
                               social_security_number_last4,
                               prt_birth_date,
                               birthdate,
                               prt_employer_code,
                               prt_telephone_nbr,
                               phone,
                               mobilephone,
                               homephone,
                               prt_zip_code,
                               dshs_provider_id,
                               prt_ss_nbr,
                               mailingpostalcode,
                               prt_first,
                               prt_last,
                               zen_age,
                               birthdate_age,
                               sex,address,
                               prt_address1,
                               prt_address2,
                               pass_criteria)
(
SELECT contactid,
       student_id,
       kn_key_name,
       firstname,
       lastname,
       prt_ss_nbr_last4,
       social_security_number_last4,
       prt_birth_date,
       birthdate,
       prt_employer_code,
       prt_telephone_nbr,
       phone, mobilephone,
       homephone,
       prt_zip_code,
       dshs_provider_id,
       prt_ss_nbr,
       mailingpostalcode,
       prt_first,
       prt_last,
       zen_age,
       birthdate_age,
       sex,
       address,
       prt_address1,
       prt_address2,
       '3'
FROM
(SELECT contactid, student_id, kn_key_name, firstname, lastname, prt_ss_nbr_last4,
social_security_number_last4, prt_birth_date, birthdate, prt_employer_code, prt_telephone_nbr,
phone, mobilephone, homephone, prt_zip_code, dshs_provider_id, prt_ss_nbr, mailingpostalcode, prt_first, prt_last, zen_age, birthdate_age,sex, address, prt_address1, prt_address2, '3',
ROW_NUMBER() OVER( PARTITION BY firstname,lastname, prt_ss_nbr, prt_birth_date, prt_zip_code
ORDER BY contactid ) AS row_num
from pstg.basehbt h join pstg.basecontact f
    /* Logic as per third pass*/
on (firstname = prt_first and lastname = prt_last)
and (length(prt_ss_nbr) >= 8)
and (prt_ss_nbr_last4 = social_security_number_last4)
and (date(birthdate) = date(prt_birth_date))
and (prt_zip_code = mailingpostalcode)
        /*Logics not in thirdpass*/
and prt_ss_nbr <> ''
and prt_birth_date is not null
and prt_ss_nbr <> '0'
and mailingpostalcode <> '0'
and mailingpostalcode <> ''
        /* Contacts not present in Person table */
and not exists(select 1 from pstg.person b where (b.firstname = f.firstname and b.lastname = f.lastname)
and (b.prt_ss_nbr_last4 = h.prt_ss_nbr_last4)
and (date(b.birthdate) = date(f.birthdate))
and (b.prt_zip_code = h.prt_zip_code))
)t
where t.row_num < 2);

/*Fourthpass logic:

Matching criteria- ((firstname = prt_first and lastname = prt_last)
or ((firstname+' '+mid_name+' '+lastname) = (prt_first+' '+prt_mid+' '+prt_last))
or ((firstname+' '+mid_name+' '+lastname) = (prt_first+' '+left(prt_mid,1)+' '+prt_last)))
and (length(prt_ss_nbr) >= 8)
and (prt_ss_nbr_last4 = social_security_number_last4)
and (date(birthdate) = date(prt_birth_date))
and records not present in person table*/



update pstg.person
set kn_key_name = b.kn_key_name,
    prt_first = b.prt_first,
    prt_last = b.prt_last,
    prt_ss_nbr_last4 = b.prt_ss_nbr_last4,
    prt_birth_date = b.prt_birth_date,
    prt_employer_code = b.prt_employer_code,
    prt_telephone_nbr = b.prt_telephone_nbr,
    prt_zip_code = b.prt_zip_code,
    prt_ss_nbr = b.prt_ss_nbr,
    lastmodified = sysdate,
    zen_age = b.zen_age,
    sex = b.sex,
    prt_address1 = b.prt_address1,
    prt_address2 = b.prt_address2
from pstg.person t
join pstg.basehbt b
on (b.prt_first = t.firstname
and b.prt_last = t.lastname
and b.prt_ss_nbr_last4 = t.social_security_number_last4
and b.prt_birth_date = t.birthdate
)
and pass_criteria like '4%';


update pstg.person
set contactid = c.contactid,
    student_id = c.student_id,
    firstname = c.firstname,
    lastname = c.lastname,
    social_security_number_last4 = c.social_security_number_last4,
    birthdate = c.birthdate,
    phone = c.phone,
    mobilephone = c.mobilephone,
    homephone = c.homephone,
    dshs_provider_id = c.dshs_provider_id,
    mailingpostalcode = c.mailingpostalcode,
    lastmodified = sysdate,
    birthdate_age = c.birthdate_age,
    address = c.address
from pstg.person b
join pstg.basecontact c
on (b.prt_first = c.firstname
and b.prt_last = c.lastname
and b.prt_ss_nbr_last4 = c.social_security_number_last4
and b.prt_birth_date = c.birthdate
)
and pass_criteria like '4%';


-- Fourth Criteria
insert into pstg.person(
                               contactid,
                               student_id,
                               kn_key_name,
                               firstname,
                               lastname,
                               prt_ss_nbr_last4,
                               social_security_number_last4,
                               prt_birth_date,
                               birthdate,
                               prt_employer_code,
                               prt_telephone_nbr,
                               phone,
                               mobilephone,
                               homephone,
                               prt_zip_code,
                               dshs_provider_id,
                               prt_ss_nbr,
                               mailingpostalcode,
                               prt_first,
                               prt_last,zen_age,
                               birthdate_age,
                               sex,address,
                               prt_address1,
                               prt_address2,
                               pass_criteria)
                                (
                                    SELECT
                                           contactid,
                                           student_id,
                                           kn_key_name,
                                           firstname,
                                           lastname,
                                           prt_ss_nbr_last4,
                                           social_security_number_last4,
                                           prt_birth_date,
                                           birthdate,
                                           prt_employer_code,
                                           prt_telephone_nbr,
                                           phone,
                                           mobilephone,
                                           homephone,
                                           prt_zip_code,
                                           dshs_provider_id,
                                           prt_ss_nbr,
                                           mailingpostalcode,
                                           prt_first,
                                           prt_last,
                                           zen_age,
                                           birthdate_age,
                                           sex,address,
                                           prt_address1,
                                           prt_address2,
                                           '4'
FROM
(SELECT contactid, student_id, kn_key_name, firstname, lastname, prt_ss_nbr_last4,
social_security_number_last4, prt_birth_date, birthdate, prt_employer_code, prt_telephone_nbr,
phone, mobilephone, homephone, prt_zip_code, dshs_provider_id, prt_ss_nbr, mailingpostalcode, prt_first, prt_last, zen_age, birthdate_age,sex, address, prt_address1, prt_address2, '4',
ROW_NUMBER() OVER( PARTITION BY firstname,lastname,prt_ss_nbr, prt_birth_date
ORDER BY contactid ) AS row_num
from pstg.basehbt h join pstg.basecontact f
    /* Logic as per requirement*/
on ((firstname = prt_first and lastname = prt_last)
or ((firstname+' '+mid_name+' '+lastname) = (prt_first+' '+prt_mid+' '+prt_last))
or ((firstname+' '+mid_name+' '+lastname) = (prt_first+' '+left(prt_mid,1)+' '+prt_last)))
and (length(prt_ss_nbr) >= 8)
and (prt_ss_nbr_last4 = social_security_number_last4)
and (date(birthdate) = date(prt_birth_date))
/*logic not in requirement*/
and prt_ss_nbr <> ''
and prt_birth_date is not null
and prt_ss_nbr <> '0'
       /* records which are not present in person table */
and not exists (select 1 from pstg.person a where (f.firstname = a.firstname and f.lastname = a.lastname)
and (h.prt_ss_nbr_last4 = a.prt_ss_nbr_last4)
and (date(h.prt_birth_date) = date(a.prt_birth_date)))
)t
where t.row_num < 2);


/*
 Fifthpass logic:
Matching criteria- ((lastname = prt_last)
or ((prt_last+' '+prt_first) = lastname))
and (length(prt_ss_nbr) >= 8)
and (prt_ss_nbr_last4 = social_security_number_last4)
and (date(birthdate) = date(prt_birth_date))
and records not present in person table
 */

update pstg.person
set kn_key_name = b.kn_key_name,
    prt_first = b.prt_first,
    prt_last = b.prt_last,
    prt_ss_nbr_last4 = b.prt_ss_nbr_last4,
    prt_birth_date = b.prt_birth_date,
    prt_employer_code = b.prt_employer_code,
    prt_telephone_nbr = b.prt_telephone_nbr,
    prt_zip_code = b.prt_zip_code,
    prt_ss_nbr = b.prt_ss_nbr,
    lastmodified = sysdate,
    zen_age = b.zen_age,
    sex = b.sex,
    prt_address1 = b.prt_address1,
    prt_address2 = b.prt_address2
from pstg.person t
join pstg.basehbt b
on (b.prt_last = t.lastname
and b.prt_ss_nbr_last4 = t.social_security_number_last4
and b.prt_birth_date = t.birthdate
)
and pass_criteria like '5%';


update pstg.person
set contactid = c.contactid,
    student_id = c.student_id,
    firstname = c.firstname,
    lastname = c.lastname,
    social_security_number_last4 = c.social_security_number_last4,
    birthdate = c.birthdate,
    phone = c.phone,
    mobilephone = c.mobilephone,
    homephone = c.homephone,
    dshs_provider_id = c.dshs_provider_id,
    mailingpostalcode = c.mailingpostalcode,
    lastmodified = sysdate,
    birthdate_age = c.birthdate_age,
    address = c.address
from pstg.person b
join pstg.basecontact c
on (b.prt_last = c.lastname
and b.prt_ss_nbr_last4 = c.social_security_number_last4
and b.prt_birth_date = c.birthdate
)
and pass_criteria like '5%';



-- Fift Criteria
insert into pstg.person(contactid,
                               student_id,
                               kn_key_name,
                               firstname,
                               lastname,
                               prt_ss_nbr_last4,
                               social_security_number_last4,
                               prt_birth_date,
                               birthdate,
                               prt_employer_code,
                               prt_telephone_nbr,
                               phone,
                               mobilephone,
                               homephone,
                               prt_zip_code,
                               dshs_provider_id,
                               prt_ss_nbr,
                               mailingpostalcode,
                               prt_first,
                               prt_last,
                               zen_age,
                               birthdate_age,
                               sex,address,
                               prt_address1,
                               prt_address2,
                               pass_criteria)
                                (
                                    SELECT contactid,
                                           student_id,
                                           kn_key_name,
                                           firstname,
                                           lastname,
                                           prt_ss_nbr_last4,
                                           social_security_number_last4,
                                           prt_birth_date,
                                           birthdate,
                                           prt_employer_code,
                                           prt_telephone_nbr,
                                           phone,
                                           mobilephone,
                                           homephone,
                                           cast(prt_zip_code as numeric(5)),
                                           dshs_provider_id,
                                           prt_ss_nbr,
                                           mailingpostalcode,
                                           prt_first,
                                           prt_last,
                                           zen_age,
                                           birthdate_age,
                                           sex,
                                           address,
                                           prt_address1,
                                           prt_address2 ,
                                           '5'
FROM
(SELECT contactid, student_id, kn_key_name, firstname, lastname, prt_ss_nbr_last4,
social_security_number_last4, prt_birth_date, birthdate, prt_employer_code, prt_telephone_nbr,
phone, mobilephone, homephone, cast(prt_zip_code as numeric(5)), dshs_provider_id, prt_ss_nbr, mailingpostalcode , prt_first, prt_last, zen_age, birthdate_age, sex, address, prt_address1, prt_address2, '5',
ROW_NUMBER() OVER( PARTITION BY lastname, prt_ss_nbr, prt_birth_date
ORDER BY contactid ) AS row_num
from pstg.basehbt h join pstg.basecontact f
    /*lOGIC AS PER FIFTH CONDITION*/
on ((lastname = prt_last)
or ((prt_last+' '+prt_first) = lastname))
and (length(prt_ss_nbr) >= 8)
and (prt_ss_nbr_last4 = social_security_number_last4)
and (date(birthdate) = date(prt_birth_date))
       /* logic not in requirement*/
and prt_ss_nbr <> ''
and prt_birth_date is not null
and prt_ss_nbr <> '0'
       /*records not present in person table*/
and not exists (select 1 from pstg.person a where ((f.lastname = a.lastname)
or ((a.prt_last+' '+a.prt_first) = (h.prt_last+' '+h.prt_first)))
and (a.prt_ss_nbr_last4 = h.prt_ss_nbr_last4)
and (date(a.birthdate) = date(f.birthdate)))
) t
where t.row_num < 2);


/*
Sixthpass logic:
Matching criteria- (firstname = prt_first and lastname = prt_last)
and (birthdate = prt_birth_date)
 */



update pstg.person
set kn_key_name = b.kn_key_name,
    prt_first = b.prt_first,
    prt_last = b.prt_last,
    prt_ss_nbr_last4 = b.prt_ss_nbr_last4,prt_birth_date = b.prt_birth_date,
    prt_employer_code = b.prt_employer_code,
    prt_telephone_nbr = b.prt_telephone_nbr,
    prt_zip_code = b.prt_zip_code,
    prt_ss_nbr = b.prt_ss_nbr,
    lastmodified = sysdate,
    zen_age = b.zen_age,
    sex = b.sex,
    prt_address1 = b.prt_address1,
    prt_address2 = b.prt_address2
from pstg.person t
join pstg.basehbt b
on (b.prt_first = t.firstname
and b.prt_last = t.lastname
and b.prt_birth_date = t.birthdate
)
and pass_criteria like '6%';


update pstg.person
set
    contactid = c.contactid,
    student_id = c.student_id,
    firstname = c.firstname,
    lastname = c.lastname,
    social_security_number_last4 = c.social_security_number_last4,
    birthdate = c.birthdate,
    phone = c.phone,
    mobilephone = c.mobilephone,
    homephone = c.homephone,
    dshs_provider_id = c.dshs_provider_id,
    mailingpostalcode = c.mailingpostalcode,
    lastmodified = sysdate,
    birthdate_age = c.birthdate_age,
    address = c.address
from pstg.person b
join pstg.basecontact c
on (b.prt_first = c.firstname
and b.prt_last = c.lastname
and b.prt_birth_date = c.birthdate
)
and pass_criteria like '6%';


-- Sixth Criteria
insert into pstg.person(contactid,
                               student_id,
                               kn_key_name,
                               firstname,
                               lastname,
                               prt_ss_nbr_last4,
                               social_security_number_last4,
                               prt_birth_date,
                               birthdate,
                               prt_employer_code,
                               prt_telephone_nbr,
                               phone,
                               mobilephone,
                               homephone,
                               prt_zip_code,
                               dshs_provider_id,
                               prt_ss_nbr,
                               mailingpostalcode,
                               prt_first,
                               prt_last,
                               zen_age,
                               birthdate_age,
                               sex,address,
                               prt_address1,
                               prt_address2,
                               pass_criteria)
(
SELECT distinct contactid,
                student_id,
                kn_key_name,
                firstname,
                lastname,
                prt_ss_nbr_last4,
                social_security_number_last4,
                prt_birth_date,
                birthdate,
                prt_employer_code,
                prt_telephone_nbr,
                phone,
                mobilephone,
                homephone,
                prt_zip_code,
                dshs_provider_id,
                prt_ss_nbr,
                mailingpostalcode,
                prt_first,
                prt_last,
                zen_age,
                birthdate_age,
                sex,
                address,
                prt_address1,
                prt_address2 ,
                '6'
FROM
(SELECT contactid, student_id, kn_key_name, firstname, lastname, prt_ss_nbr_last4,
social_security_number_last4, prt_birth_date, birthdate, prt_employer_code, prt_telephone_nbr,
phone, mobilephone, homephone, prt_zip_code, dshs_provider_id, prt_ss_nbr, mailingpostalcode, prt_first, prt_last, zen_age, birthdate_age, sex, address, prt_address1, prt_address2, '6',
ROW_NUMBER() OVER( PARTITION BY firstname, lastname, prt_birth_date
ORDER BY contactid ) AS row_num
FROM pstg.basehbt h join pstg.basecontact f
    /* Logic as per requirement */
on (firstname = prt_first and lastname = prt_last)
and (birthdate = prt_birth_date)
and prt_birth_date is not null
 /* Logic not in  requirement */
and not exists (select 1 from pstg.person a where (h.prt_first = a.prt_first and h.prt_last = a.prt_last)
and (h.prt_birth_date = a.prt_birth_date))
)t
where t.row_num < 2);

/*
Seventhpass logic:
Matching criteria- (firstname = prt_first and lastname = prt_last)
and (length(prt_ss_nbr) >= 8)
and (prt_ss_nbr_last4 = social_security_number_last4)
 */


update pstg.person
set kn_key_name = b.kn_key_name,
    prt_first = b.prt_first,
    prt_last = b.prt_last,
    prt_ss_nbr_last4 = b.prt_ss_nbr_last4,
    prt_birth_date = b.prt_birth_date,
    prt_employer_code = b.prt_employer_code,
    prt_telephone_nbr = b.prt_telephone_nbr,
    prt_zip_code = b.prt_zip_code,
    prt_ss_nbr = b.prt_ss_nbr,
    lastmodified = sysdate,
    zen_age = b.zen_age,
    sex = b.sex,
    prt_address1 = b.prt_address1,
    prt_address2 = b.prt_address2
from pstg.person t
join pstg.basehbt b
on (b.prt_first = t.firstname
         /* Missing Logic added */
        and (length(b.prt_ss_nbr) >= 8)
and b.prt_last = t.lastname
and b.prt_ss_nbr_last4 = t.social_security_number_last4
)
and pass_criteria like '7%';


update pstg.person
set contactid = c.contactid,
    student_id = c.student_id,
    firstname = c.firstname,
    lastname = c.lastname,
    social_security_number_last4 = c.social_security_number_last4,
    birthdate = c.birthdate,
    phone = c.phone,
    mobilephone = c.mobilephone,
    homephone = c.homephone,
    dshs_provider_id = c.dshs_provider_id,
    mailingpostalcode = c.mailingpostalcode,
    lastmodified = sysdate,
    birthdate_age = c.birthdate_age,
    address = c.address
from pstg.person b
join pstg.basecontact c
on (b.prt_first = c.firstname
and b.prt_last = c.lastname
         /* Missing Logic added */
        and (length(b.prt_ss_nbr) >= 8)
and b.prt_ss_nbr_last4 = c.social_security_number_last4
)
and pass_criteria like '7%';



-- Seven Criteria
insert into pstg.person(contactid,
                               student_id,
                               kn_key_name,
                               firstname,
                               lastname,
                               prt_ss_nbr_last4,
                               social_security_number_last4,
                               prt_birth_date,
                               birthdate,
                               prt_employer_code,
                               prt_telephone_nbr,
                               phone,
                               mobilephone,
                               homephone,
                               prt_zip_code,
                               dshs_provider_id,
                               prt_ss_nbr,
                               mailingpostalcode,
                               prt_first,
                               prt_last,
                               zen_age,
                               birthdate_age,
                               sex,
                               address,
                               prt_address1,
                               prt_address2,
                               pass_criteria)
(
SELECT distinct contactid, student_id, kn_key_name, firstname, lastname, prt_ss_nbr_last4,social_security_number_last4, prt_birth_date, birthdate, prt_employer_code, prt_telephone_nbr,phone, mobilephone, homephone, prt_zip_code, dshs_provider_id, prt_ss_nbr, mailingpostalcode, prt_first, prt_last, zen_age, birthdate_age, sex, address, prt_address1, prt_address2 , '7'
FROM
(SELECT contactid, student_id, kn_key_name, firstname, lastname, prt_ss_nbr_last4,
social_security_number_last4, prt_birth_date, birthdate, prt_employer_code, prt_telephone_nbr,
phone, mobilephone, homephone, prt_zip_code, dshs_provider_id, prt_ss_nbr, mailingpostalcode, prt_first, prt_last, zen_age, birthdate_age, sex, address, prt_address1, prt_address2, '7',
ROW_NUMBER() OVER( PARTITION BY firstname, lastname, prt_ss_nbr
ORDER BY contactid ) AS row_num
FROM pstg.basehbt h join pstg.basecontact f
        /* Logic as per requirement */
on (firstname = prt_first and lastname = prt_last)
and (length(prt_ss_nbr) >= 8)
and (prt_ss_nbr_last4 = social_security_number_last4)
           /* Logic not in  requirement */
and prt_ss_nbr <> ''
and prt_ss_nbr <> '0'
and not exists (select 1 from pstg.person a where (h.prt_first = a.prt_first and h.prt_last = a.prt_last)
and (h.prt_ss_nbr_last4 = a.prt_ss_nbr_last4))
)t
where t.row_num < 2);

/*
 Eighthpass logic:
Matching criteria- (firstname = prt_first)
and (length(prt_ss_nbr) >= 8)
and (prt_ss_nbr_last4 = social_security_number_last4)
and (date(birthdate) = date(prt_birth_date))
and (prt_zip_code = mailingpostalcode)
 */

update pstg.person
set kn_key_name = b.kn_key_name,
    prt_first = b.prt_first,
    prt_last = b.prt_last,
    prt_ss_nbr_last4 = b.prt_ss_nbr_last4,
    prt_birth_date = b.prt_birth_date,
    prt_employer_code = b.prt_employer_code,
    prt_telephone_nbr = b.prt_telephone_nbr,
    prt_zip_code = b.prt_zip_code,
    prt_ss_nbr = b.prt_ss_nbr,
    lastmodified = sysdate,
    zen_age = b.zen_age,
    sex = b.sex,
    prt_address1 = b.prt_address1,
    prt_address2 = b.prt_address2
from pstg.person t
join pstg.basehbt b
on (b.prt_first = t.firstname
        /* Missing Logic added */
        and (length(b.prt_ss_nbr) >= 8)
and b.prt_ss_nbr_last4 = t.social_security_number_last4
and b.prt_birth_date = t.birthdate
and b.prt_zip_code = t.mailingpostalcode
)
and pass_criteria like '8%';


update pstg.person
set contactid = c.contactid,
    student_id = c.student_id,
    firstname = c.firstname,
    lastname = c.lastname,
    social_security_number_last4 = c.social_security_number_last4,
    birthdate = c.birthdate,
    phone = c.phone,
    mobilephone = c.mobilephone,
    homephone = c.homephone,
    dshs_provider_id = c.dshs_provider_id,
    mailingpostalcode = c.mailingpostalcode,
    lastmodified = sysdate,
    birthdate_age = c.birthdate_age,
    address = c.address
from pstg.person b
join pstg.basecontact c
on (b.prt_first = c.firstname
        /* Missing Logic added */
        and (length(b.prt_ss_nbr) >= 8)
and b.prt_ss_nbr_last4 = c.social_security_number_last4
and b.prt_birth_date = c.birthdate
and b.prt_zip_code = c.mailingpostalcode)
and pass_criteria like '8%';


-- Eight Criteria
insert into pstg.person(contactid,
                               student_id,
                               kn_key_name,
                               firstname,
                               lastname,
                               prt_ss_nbr_last4,
                               social_security_number_last4,
                               prt_birth_date, birthdate,
                               prt_employer_code,
                               prt_telephone_nbr,
                               phone,
                               mobilephone,
                               homephone,
                               prt_zip_code,
                               dshs_provider_id,
                               prt_ss_nbr,
                               mailingpostalcode,
                               prt_first,
                               prt_last,
                               zen_age,
                               birthdate_age,
                               sex,address,
                               prt_address1,
                               prt_address2,
                               pass_criteria)
(
SELECT contactid,
       student_id,
       kn_key_name,
       firstname,
       lastname,
       prt_ss_nbr_last4,
       social_security_number_last4,
       prt_birth_date,
       birthdate,
       prt_employer_code,
       prt_telephone_nbr,
       phone,
       mobilephone,
       homephone,
       prt_zip_code,
       dshs_provider_id,
       prt_ss_nbr,
       mailingpostalcode,
       prt_first,
       prt_last,
       zen_age,
       birthdate_age,
       sex,
       address,
       prt_address1,
       prt_address2,
       '8'
FROM
(SELECT contactid, student_id, kn_key_name, firstname, lastname, prt_ss_nbr_last4,
social_security_number_last4, prt_birth_date, birthdate, prt_employer_code, prt_telephone_nbr,
phone, mobilephone, homephone, prt_zip_code, dshs_provider_id, prt_ss_nbr, mailingpostalcode, prt_first, prt_last, zen_age, birthdate_age,sex, address, prt_address1, prt_address2, '8',
ROW_NUMBER() OVER( PARTITION BY firstname, prt_ss_nbr, prt_birth_date, prt_zip_code
ORDER BY contactid ) AS row_num
from pstg.basehbt h join pstg.basecontact f
    /* Logic as per requirement */
on (firstname = prt_first)
and (length(prt_ss_nbr) >= 8)
and (prt_ss_nbr_last4 = social_security_number_last4)
and (date(birthdate) = date(prt_birth_date))
and (prt_zip_code = mailingpostalcode)
       /* Logic not in requirement */
and prt_ss_nbr <> ''
and prt_birth_date is not null
and prt_ss_nbr <> '0'
and mailingpostalcode <> '0'
and mailingpostalcode <> ''
and not exists (select 1 from pstg.person a where (h.prt_first = a.prt_first)
and (h.prt_ss_nbr_last4 = a.prt_ss_nbr_last4)
and (date(h.prt_birth_date) = date(a.prt_birth_date))
and (h.prt_zip_code = a.prt_zip_code))
) t
where t.row_num < 2);


/*
Ninthpass logic:
Matching criteria- (firstname = prt_first and lastname = prt_last)
and (prt_zip_code = mailingpostalcode)
and (address = prt_address1
or address = prt_address2
or address = (prt_address1+' '+prt_address2)
or address = (prt_address2+' '+prt_address1))
 */

update pstg.person
set kn_key_name = b.kn_key_name,
    prt_first = b.prt_first,
    prt_last = b.prt_last,
    prt_ss_nbr_last4 = b.prt_ss_nbr_last4,
    prt_birth_date = b.prt_birth_date,
    prt_employer_code = b.prt_employer_code,
    prt_telephone_nbr = b.prt_telephone_nbr,
    prt_zip_code = b.prt_zip_code,
    prt_ss_nbr = b.prt_ss_nbr,
    lastmodified = sysdate,
    zen_age = b.zen_age,
    sex = b.sex,
    prt_address1 = b.prt_address1,
    prt_address2 = b.prt_address2
from pstg.person t
join pstg.basehbt b
on (b.prt_first = t.firstname
and b.prt_last = t.lastname
and b.prt_zip_code = t.mailingpostalcode
and (b.prt_address1 = t.address
or b.prt_address2 = t.address
or (b.prt_address1+' '+b.prt_address2) = t.address
or (b.prt_address2+' '+b.prt_address1) = t.address
)
and pass_criteria like '9%'
);

update pstg.person
set contactid = c.contactid,
    student_id = c.student_id,
    firstname = c.firstname,
    lastname = c.lastname,
    social_security_number_last4 = c.social_security_number_last4,
    birthdate = c.birthdate,
    phone = c.phone,
    mobilephone = c.mobilephone,
    homephone = c.homephone,
    dshs_provider_id = c.dshs_provider_id,
    mailingpostalcode = c.mailingpostalcode,
    lastmodified = sysdate,
    birthdate_age = c.birthdate_age,
    address = c.address
from pstg.person b
join pstg.basecontact c
on (b.prt_first = c.firstname
and b.prt_last = c.lastname
and (b.prt_address1 = c.address
or b.prt_address2 = c.address
or (b.prt_address1+' '+b.prt_address2) = c.address
or (b.prt_address2+' '+b.prt_address1) = c.address
)
and pass_criteria like '9%'
);



-- Ninth Criteria
insert into pstg.person(contactid,
                               student_id,
                               kn_key_name,
                               firstname,
                               lastname,
                               prt_ss_nbr_last4,
                               social_security_number_last4,
                               prt_birth_date, birthdate,
                               prt_employer_code,
                               prt_telephone_nbr,
                               phone,
                               mobilephone,
                               homephone,
                               prt_zip_code,
                               dshs_provider_id,
                               prt_ss_nbr,
                               mailingpostalcode,
                               prt_first,
                               prt_last,zen_age,
                               birthdate_age,
                               sex,
                               address,
                               prt_address1,
                               prt_address2,
                               pass_criteria)
(
SELECT contactid,
       student_id,
       kn_key_name,
       firstname,
       lastname,
       prt_ss_nbr_last4,
       social_security_number_last4,
       prt_birth_date,
       birthdate,
       prt_employer_code,
       prt_telephone_nbr,phone,
       mobilephone,
       homephone,
       prt_zip_code,
       dshs_provider_id,
       prt_ss_nbr,
       mailingpostalcode,
       prt_first,
       prt_last,
       zen_age,
       birthdate_age,
       sex,
       address,
       prt_address1,
       prt_address2,
       '9'
FROM
(SELECT contactid, student_id, kn_key_name, firstname, lastname, prt_ss_nbr_last4,
social_security_number_last4, prt_birth_date, birthdate, prt_employer_code, prt_telephone_nbr,
phone, mobilephone, homephone, prt_zip_code, dshs_provider_id, prt_ss_nbr, mailingpostalcode, prt_first, prt_last, zen_age, birthdate_age,sex, address, prt_address1, prt_address2, '9',
ROW_NUMBER() OVER( PARTITION BY firstname,lastname, prt_zip_code, address
ORDER BY contactid ) AS row_num
from pstg.basehbt h join pstg.basecontact f
    /*Logic as per requirement */
on (firstname = prt_first and lastname = prt_last)
and (prt_zip_code = mailingpostalcode)
and (address = prt_address1
or address = prt_address2
or address = (prt_address1+' '+prt_address2)
or address = (prt_address2+' '+prt_address1)
)
       /*Logic not in requirmeent */
and address <> ''
and address <> '0'
and mailingpostalcode <> '0'
and mailingpostalcode <> ''
and not exists (select 1 from pstg.person a where (h.prt_first = a.prt_first and h.prt_last = a.prt_last)
and (h.prt_zip_code = a.prt_zip_code)
and (h.prt_address1 = a.prt_address1
or h.prt_address2 = a.prt_address2
or (h.prt_address1+' '+h.prt_address2) = (a.prt_address1+' '+a.prt_address2)
or (h.prt_address2+' '+h.prt_address1) = (a.prt_address2+' '+a.prt_address1)
))
)t
where t.row_num < 2);

/*
HBT nonmatched records logic:
Matching criteria- (select  * from pstg.basehbt b
where not exists(select 1 from pstg.person t where b.prt_ss_nbr = t.prt_ss_nbr and b.kn_key_name = t.kn_key_name)
and prt_first <> ''
and prt_last <> '')*/


--HBT nonmatched records
insert into pstg.person(kn_key_name,
                               prt_ss_nbr_last4,
                               prt_birth_date,
                               prt_employer_code,
                               prt_telephone_nbr,
                               prt_zip_code,
                               prt_ss_nbr,
                               prt_first,
                               prt_last,
                               zen_age,
                               sex,
                               prt_address1,
                               prt_address2,
                               pass_criteria)
(select kn_key_name, right(prt_ss_nbr, 4) as prt_ss_nbr_last4, prt_birth_date, prt_employer_code, prt_telephone_nbr, prt_zip_code, prt_ss_nbr, prt_first, prt_last,zen_age, sex, prt_address1, prt_address2, 'NULL'
from pstg.basehbt b
where not exists(select 1 from pstg.person t where b.prt_ss_nbr = t.prt_ss_nbr and b.kn_key_name = t.kn_key_name)
and prt_first <> ''
and prt_last <> '');


/*
Contact nonmatched records logic:
Matching criteria- (select  * from pstg.basecontact f
where not exists (select 1 from pstg.person t where f.student_id = t.student_id))
 */

--Contact nonmatched records logic:
insert into pstg.person(contactid,
                               student_id,
                               firstname,
                               lastname,
                               social_security_number_last4,
                               birthdate,
                               phone,
                               mobilephone,
                               homephone,
                               dshs_provider_id,
                               mailingpostalcode,
                               birthdate_age,
                               address,
                               pass_criteria)
(
select contactid, student_id, firstname, lastname,
social_security_number_last4, birthdate,
phone, mobilephone, homephone, dshs_provider_id, mailingpostalcode, birthdate_age, address, 'NULL'
/* Changed the logic from contact id to student id as per requirements */
from pstg.basecontact f where not exists (select 1 from pstg.person t where f.contactid= t.contactid)
)
;

update pstg.person set pass_criteria = split_part(pass_criteria, '.', 1) from pstg.person where pass_criteria LIKE '%.%';

update pstg.person
set pass_criteria = (case when n.pass_criteria = 'NULL' then '0'
when  n.pass_criteria =0 then '0'
 else n.pass_criteria ||'.'||row_id end)
from
(select *, row_number() over (partition by contactid) row_id
from
(select *,
count(contactid) over (partition by contactid, pass_criteria) rn
from pstg.person
where contactid in (Select contactid from pstg.person pk group by pk.contactid having count(pk.contactid) > 1))
where rn > 1) n
join pstg.person p on p.prt_ss_nbr = n.prt_ss_nbr;

update pstg.person
set pass_criteria = (case when n.pass_criteria = 'NULL' then '0' 
when  n.pass_criteria =0 then '0'
else n.pass_criteria  ||'.'||row_id end)
from
(select *, row_number() over (partition by kn_key_name) row_id
from
(select *,
count(kn_key_name) over (partition by kn_key_name, pass_criteria) rn
from pstg.person
where kn_key_name in (Select kn_key_name from pstg.person pk group by pk.kn_key_name having count(pk.kn_key_name) > 1))
where rn > 1) n
join pstg.person p on p.contactid = n.contactid;


/*Reset*/
update pstg.person set personid=NULL;
--update pstg.person set keyindicator= 'false';

/*First logic : Update Personid for contactid duplicates set personid= m.bgidkey*/

update pstg.person
set personid= m.bgidkey
from
(select * from
(select *, row_number() over (partition by contactid order by pass_criteria) as rn
from pstg.person
/*Find duplicate for contactid */
where contactid in (select contactid from pstg.person group by contactid having count(contactid ) > 1)) t
where t.rn = 1) m
join pstg.person x on x.contactid = m.contactid
and x.bgidkey != m.bgidkey
where m.contactid in
(select contactid from
(select *, row_number() over (partition by contactid order by pass_criteria) as rn
from pstg.person
where contactid in (select contactid from pstg.person group by contactid having count(contactid ) > 1)) t
where t.rn > 1
);

/*Second logic : Update Personid for kn_key_name duplicates set personid= m.bgidkey*/
update pstg.person
set personid= m.bgidkey
from
(select * from
(select *, row_number() over (partition by kn_key_name order by pass_criteria) as rn
from pstg.person
/*Find duplicate for fn_key_name */
where kn_key_name in (select kn_key_name from pstg.person group by kn_key_name having count(kn_key_name ) > 1)) t
where t.rn = 1) m
join pstg.person x on x.kn_key_name = m.kn_key_name
and x.bgidkey != m.bgidkey
where m.kn_key_name in
(select kn_key_name from
(select *, row_number() over (partition by kn_key_name order by pass_criteria) as rn
from pstg.person
where kn_key_name in (select kn_key_name from pstg.person group by kn_key_name having count(kn_key_name ) > 1)) t
where t.rn > 1
);



/* Third Logic : Update Personid with the default bgidkey for non duplicates set personid= m.bgidkey*/
update pstg.person set personid = bgidkey where personid is null;

/* Fourth Logic : Update keyindicator for duplicates set keyindicator = 'true'*/
update pstg.person
set keyindicator= 'true'
from pstg.person t
where kn_key_name in
      /* Check for duplicates with contact id >1 or by kn_key_name>1*/
(select kn_key_name from pstg.person group by kn_key_name having count(kn_key_name) > 1)
or contactid in
(select contactid from pstg.person group by contactid having count(contactid) > 1);

/* Fifth Logic : Update keyindicator for non duplicates set keyindicator = 'false'*/

update pstg.person set keyindicator = 'false' from pstg.person t where keyindicator is null;
END;
$$

