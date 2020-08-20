CREATE OR REPLACE PROCEDURE pstg.load_basetables()
 LANGUAGE plpgsql
AS $$
BEGIN
----Pull everyone from SF contact who is not already in the person table, excluding test records
insert into  pstg.basecontact(
SELECT DISTINCT
                contactid,
                student_id,
                /*Logic :  firstname =  regexp_replace((upper(firstname::text)), '[^ a-zA-Z]', ' ')*/
                regexp_replace((upper(firstname::text)), '[^ a-zA-Z]', ' ') AS firstname,
                /*Logic : lastname = regexp_replace((upper(lastname::text)), '[^ a-zA-Z]', ' ')*/
                regexp_replace((upper(lastname::text)), '[^ a-zA-Z]', ' ') AS lastname,
                social_security_number_last4,
                birthdate,
                ltrim(rtrim(COALESCE(
                    CASE
                        WHEN "replace"("replace"("replace"("replace"("replace"(phone::text, '-'::character varying::text, ''::character varying::text), '('::character varying::text, ''::character varying::text), ')'::character varying::text, ''::character varying::text), ' '::character varying::text, ''::character varying::text), '[^0-9]'::character varying::text, ''::character varying::text) = ''::character varying::text THEN NULL::character varying::text
                        ELSE "replace"("replace"("replace"("replace"("replace"(phone::text, '-'::character varying::text, ''::character varying::text), '('::character varying::text, ''::character varying::text), ')'::character varying::text, ''::character varying::text), ' '::character varying::text, ''::character varying::text), '[^0-9]'::character varying::text, ''::character varying::text)END, '0'::character varying::text)))
                    AS phone,
                ltrim(rtrim(COALESCE(CASE WHEN "replace"("replace"("replace"("replace"("replace"(mobilephone::text, '-'::character varying::text, ''::character varying::text), '('::character varying::text, ''::character varying::text), ')'::character varying::text, ''::character varying::text), ' '::character varying::text, ''::character varying::text), '[^0-9]'::character varying::text, ''::character varying::text) = ''::character varying::text THEN NULL::character varying::text
                ELSE "replace"("replace"("replace"("replace"("replace"(mobilephone::text, '-'::character varying::text, ''::character varying::text), '('::character varying::text, ''::character varying::text), ')'::character varying::text, ''::character varying::text), ' '::character varying::text, ''::character varying::text), '[^0-9]'::character varying::text, ''::character varying::text)END, '0'::character varying::text)))
                    AS mobilephone,
                ltrim(rtrim(COALESCE(
                    CASE
                        WHEN "replace"("replace"("replace"("replace"("replace"(homephone::text, '-'::character varying::text, ''::character varying::text), '('::character varying::text, ''::character varying::text), ')'::character varying::text, ''::character varying::text), ' '::character varying::text, ''::character varying::text), '[^0-9]'::character varying::text, ''::character varying::text) = ''::character varying::text THEN NULL::character varying::text
                        ELSE "replace"("replace"("replace"("replace"("replace"(homephone::text, '-'::character varying::text, ''::character varying::text), '('::character varying::text, ''::character varying::text), ')'::character varying::text, ''::character varying::text), ' '::character varying::text, ''::character varying::text), '[^0-9]'::character varying::text, ''::character varying::text) END, '0'::character varying::text)))
                    AS homephone,
                /*Logic : dshs_provider_id = substring(dshs_provider_id::text, 1, 7)*/
                "substring"(dshs_provider_id::text, 1, 7) AS dshs_provider_id,
                mailingpostalcode, status,
                /*Logic : birthdate_age = ((datediff(year, birthdate, sysdate))-1)*/
                date_part(y,sysdate)-date_part(y,birthdate::date) as birthdate_age ,
                ---((datediff(year, birthdate, sysdate))-1) as birthdate_age,
                /*Logic : mailingstreet = upper(mailingstreet)*/
                upper(mailingstreet) AS mailingstreet,
                middle_initial
FROM acquisition.sf_contact c
where not exists (select 1 from pstg.person t 
where c.contactid= t.contactid
and c.student_id=t.student_id
and regexp_replace((upper(c.firstname::text)), '[^ a-zA-Z]', ' ')=t.firstname
and regexp_replace((upper(c.lastname::text)), '[^ a-zA-Z]', ' ')=t.lastname
and c.social_security_number_last4=t.social_security_number_last4
and coalesce(c.birthdate::varchar ,'') = coalesce(t.birthdate::varchar ,'')
and ltrim(rtrim(COALESCE(
                    CASE
                        WHEN "replace"("replace"("replace"("replace"("replace"(c.phone::text, '-'::character varying::text, ''::character varying::text), '('::character varying::text, ''::character varying::text), ')'::character varying::text, ''::character varying::text), ' '::character varying::text, ''::character varying::text), '[^0-9]'::character varying::text, ''::character varying::text) = ''::character varying::text THEN NULL::character varying::text
                        ELSE "replace"("replace"("replace"("replace"("replace"(c.phone::text, '-'::character varying::text, ''::character varying::text), '('::character varying::text, ''::character varying::text), ')'::character varying::text, ''::character varying::text), ' '::character varying::text, ''::character varying::text), '[^0-9]'::character varying::text, ''::character varying::text)END, '0'::character varying::text)))
                    =t.phone
and ltrim(rtrim(COALESCE(CASE WHEN "replace"("replace"("replace"("replace"("replace"(c.mobilephone::text, '-'::character varying::text, ''::character varying::text), '('::character varying::text, ''::character varying::text), ')'::character varying::text, ''::character varying::text), ' '::character varying::text, ''::character varying::text), '[^0-9]'::character varying::text, ''::character varying::text) = ''::character varying::text THEN NULL::character varying::text
                ELSE "replace"("replace"("replace"("replace"("replace"(c.mobilephone::text, '-'::character varying::text, ''::character varying::text), '('::character varying::text, ''::character varying::text), ')'::character varying::text, ''::character varying::text), ' '::character varying::text, ''::character varying::text), '[^0-9]'::character varying::text, ''::character varying::text)END, '0'::character varying::text)))
                    =t.mobilephone
and ltrim(rtrim(COALESCE(
                    CASE
                        WHEN "replace"("replace"("replace"("replace"("replace"(c.homephone::text, '-'::character varying::text, ''::character varying::text), '('::character varying::text, ''::character varying::text), ')'::character varying::text, ''::character varying::text), ' '::character varying::text, ''::character varying::text), '[^0-9]'::character varying::text, ''::character varying::text) = ''::character varying::text THEN NULL::character varying::text
                        ELSE "replace"("replace"("replace"("replace"("replace"(c.homephone::text, '-'::character varying::text, ''::character varying::text), '('::character varying::text, ''::character varying::text), ')'::character varying::text, ''::character varying::text), ' '::character varying::text, ''::character varying::text), '[^0-9]'::character varying::text, ''::character varying::text) END, '0'::character varying::text)))
                    =t.homephone
and "substring"(c.dshs_provider_id::text, 1, 7)=t.dshs_provider_id
and COALESCE(c.mailingpostalcode,'')=t.mailingpostalcode
--and date_part(y,sysdate)-date_part(y,c.birthdate::date)=t.birthdate_age
)
and firstname not like '%*%'
and firstname not like '%@%'
and upper(firstname) not like 'TEST'
and upper(firstname) not like '%T*ST%'
/*added this logic*/
and firstname<>''
and UPPER(firstname) not like 'DUP %'
and UPPER(firstname) not like 'DUP%'
/*Logic : lastname <> '%*%', '%@%', '%T*ST%', 'TEST',''*/
and lastname not like '%*%'
and lastname not like '%@%'
and upper(lastname) not like '%T*ST%'
and upper(lastname) not like 'TEST'
/*added this logic*/
and lastname<>''
/*Logic : upper(status) <> 'DUPLICATE'*/
and upper(status) not like 'DUPLICATE'
and status <>''
);

-- Pull everyone from hbt who is not in the person table, and put them in basehbt

insert into pstg.basehbt(
select distinct
       kn_key_name,
       /*prt_first = regexp_replace((upper(prt_first)), '[^ a-zA-Z]', ' ')*/
       regexp_replace((upper(prt_first)), '[^ a-zA-Z]', ' ') as prt_first,
       /*prt_last = regexp_replace((upper(prt_last)), '[^ a-zA-Z]', ' ')*/
      regexp_replace((upper(prt_last)), '[^ a-zA-Z]', ' ') as prt_last,
       /*prt_ss_nbr_last4 = right(prt_ss_nbr, 4)*/
       right(prt_ss_nbr, 4) as prt_ss_nbr_last4,
       prt_birth_date,
       prt_employer_code,
       prt_telephone_nbr,
       /*prt_zip_code = cast(prt_zip_code as varchar(20))*/
       cast(prt_zip_code as varchar(20)) as prt_zip_code ,
       prt_ss_nbr,
       /*zen_age = ((datediff(year, prt_birth_date, sysdate))-1)*/
       date_part(y,sysdate)-date_part(y,prt_birth_date::date) as zen_age ,
       --used date part to calculate the difference
       ---((datediff(year, prt_birth_date, sysdate))-1) as zen_age
       prt_sex as sex,
       /*prt_address1 = upper(prt_addr1)*/
       upper(prt_addr1) as prt_address1,
       /*prt_address2 = upper(prt_addr2)*/
       upper(prt_addr2) as prt_address2,
       prt_middle
from hbt.provider p join hbt.demographic d
--added cast to numeric as data types are different
on kn_number = cast(prt_ss_nbr as numeric)
where not exists (select 1 from pstg.person t 
where coalesce(t.prt_ss_nbr,'') = coalesce(d.prt_ss_nbr,'')
and coalesce(p.kn_key_name,'')=coalesce(t.kn_key_name,'')
and coalesce(regexp_replace((upper(d.prt_first)), '[^ a-zA-Z]', ' '),'')=coalesce(t.prt_first,'')
and coalesce(regexp_replace((upper(d.prt_last)), '[^ a-zA-Z]', ' '),'')=coalesce(t.prt_last,'')
and coalesce(right(d.prt_ss_nbr, 4),'')=coalesce(t.prt_ss_nbr_last4,'')
and coalesce(d.prt_birth_date::varchar,'')=coalesce(t.prt_birth_date::varchar,'')
and coalesce(d.prt_employer_code::varchar,'')=coalesce(t.prt_employer_code::varchar,'')
and coalesce(d.prt_telephone_nbr::varchar,'')=coalesce(t.prt_telephone_nbr::varchar,'')
and coalesce(d.prt_zip_code::varchar,'')=coalesce(t.prt_zip_code::varchar,'')
and coalesce(d.prt_ss_nbr::varchar,'')=coalesce(t.prt_ss_nbr::varchar,'')
and coalesce(d.prt_sex::varchar,'')=coalesce(t.sex::varchar,'')
and coalesce(upper(d.prt_addr1),'')=coalesce(t.prt_address1,'')
and coalesce(upper(d.prt_addr2),'')=coalesce(t.prt_address2,'')
));

END;
$$
