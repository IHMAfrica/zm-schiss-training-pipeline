SELECT
    r.id AS staffid,
    CONCAT(r.first_name, ' ', r.last_name) AS fullname,
    r.reg_date,
    r.decision,
    r.training_type,
    r.staff_type,
    r.nrc,
    r.sex,
    r.age,
    r.`role` AS job_role,
    r.education,
    r.used_smartcare,
    r.currently_using,
    s.stakeholder_name,
    f.facility_name,
    pr.province_name,
    d.district_name,

    /* === Derived columns replacing the UPDATE logic === */
    CASE
        WHEN r.reg_date < '2024-10-01' THEN '2024'
        ELSE '2025'
    END AS financialyear,

    CASE
        WHEN MONTH(r.reg_date) IN (10,11,12) THEN 'Q1'
        WHEN MONTH(r.reg_date) IN (1,2,3)     THEN 'Q2'
        WHEN MONTH(r.reg_date) IN (4,5,6)     THEN 'Q3'
        WHEN MONTH(r.reg_date) IN (7,8,9)     THEN 'Q4'
        ELSE NULL
    END AS quarters,

    CASE UPPER(TRIM(pr.province_name))
        WHEN 'LUSAKA'        THEN 'LSK'
        WHEN 'COPPERBELT'    THEN 'CBP'
        WHEN 'EASTERN'       THEN 'EP'
        WHEN 'CENTRAL'       THEN 'CP'
        WHEN 'NORTH-WESTERN' THEN 'NW'
        WHEN 'MUCHINGA'      THEN 'MP'
        WHEN 'WESTERN'       THEN 'WP'
        WHEN 'LUAPULA'       THEN 'LP'
        WHEN 'SOUTHERN'      THEN 'SP'
        WHEN 'NORTHERN'      THEN 'NP'
        ELSE NULL
    END AS isocode,

    /* === Pre-test (10 questions) === */
    prt.test_date AS pretestdate,
      (CASE WHEN prt.question_1  = prt.answer_1  THEN 1 ELSE 0 END
     + CASE WHEN prt.question_2  = prt.answer_2  THEN 1 ELSE 0 END
     + CASE WHEN prt.question_3  = prt.answer_3  THEN 1 ELSE 0 END
     + CASE WHEN prt.question_4  = prt.answer_4  THEN 1 ELSE 0 END
     + CASE WHEN prt.question_5  = prt.answer_5  THEN 1 ELSE 0 END
     + CASE WHEN prt.question_6  = prt.answer_6  THEN 1 ELSE 0 END
     + CASE WHEN prt.question_7  = prt.answer_7  THEN 1 ELSE 0 END
     + CASE WHEN prt.question_8  = prt.answer_8  THEN 1 ELSE 0 END
     + CASE WHEN prt.question_9  = prt.answer_9  THEN 1 ELSE 0 END
     + CASE WHEN prt.question_10 = prt.answer_10 THEN 1 ELSE 0 END) AS pretestscore,

    /* === Post-test (10 questions) === */
    pt.test_date AS posttestdate,
      (CASE WHEN pt.question_1  = pt.answer_1  THEN 1 ELSE 0 END
     + CASE WHEN pt.question_2  = pt.answer_2  THEN 1 ELSE 0 END
     + CASE WHEN pt.question_3  = pt.answer_3  THEN 1 ELSE 0 END
     + CASE WHEN pt.question_4  = pt.answer_4  THEN 1 ELSE 0 END
     + CASE WHEN pt.question_5  = pt.answer_5  THEN 1 ELSE 0 END
     + CASE WHEN pt.question_6  = pt.answer_6  THEN 1 ELSE 0 END
     + CASE WHEN pt.question_7  = pt.answer_7  THEN 1 ELSE 0 END
     + CASE WHEN pt.question_8  = pt.answer_8  THEN 1 ELSE 0 END
     + CASE WHEN pt.question_9  = pt.answer_9  THEN 1 ELSE 0 END
     + CASE WHEN pt.question_10 = pt.answer_10 THEN 1 ELSE 0 END) AS posttestscore,

    /* === Manager pre-test (8 questions) === */
    mt.test_date AS mgr_pretestdate,
      (CASE WHEN mt.question_1 = mt.answer_1 THEN 1 ELSE 0 END
     + CASE WHEN mt.question_2 = mt.answer_2 THEN 1 ELSE 0 END
     + CASE WHEN mt.question_3 = mt.answer_3 THEN 1 ELSE 0 END
     + CASE WHEN mt.question_4 = mt.answer_4 THEN 1 ELSE 0 END
     + CASE WHEN mt.question_5 = mt.answer_5 THEN 1 ELSE 0 END
     + CASE WHEN mt.question_6 = mt.answer_6 THEN 1 ELSE 0 END
     + CASE WHEN mt.question_7 = mt.answer_7 THEN 1 ELSE 0 END
     + CASE WHEN mt.question_8 = mt.answer_8 THEN 1 ELSE 0 END) AS mgr_pretestscore,

    /* === Manager post-test (8 questions) === */
    mpt.test_date AS mgr_posttestdate,
      (CASE WHEN mpt.question_1 = mpt.answer_1 THEN 1 ELSE 0 END
     + CASE WHEN mpt.question_2 = mpt.answer_2 THEN 1 ELSE 0 END
     + CASE WHEN mpt.question_3 = mpt.answer_3 THEN 1 ELSE 0 END
     + CASE WHEN mpt.question_4 = mpt.answer_4 THEN 1 ELSE 0 END
     + CASE WHEN mpt.question_5 = mpt.answer_5 THEN 1 ELSE 0 END
     + CASE WHEN mpt.question_6 = mpt.answer_6 THEN 1 ELSE 0 END
     + CASE WHEN mpt.question_7 = mpt.answer_7 THEN 1 ELSE 0 END
     + CASE WHEN mpt.question_8 = mpt.answer_8 THEN 1 ELSE 0 END) AS mgr_posttestscore

FROM registration r
LEFT JOIN province     pr  ON r.province_id   = pr.id
LEFT JOIN district     d   ON r.district_id   = d.id
LEFT JOIN facility     f   ON r.facility_id   = f.id
LEFT JOIN stakeholders s   ON r.stakeholder_id= s.id
LEFT JOIN pre_test     prt ON r.id            = prt.id
LEFT JOIN post_test    pt  ON r.id            = pt.id
LEFT JOIN manager_test mt  ON r.id            = mt.id
LEFT JOIN manager_post_test mpt ON r.id       = mpt.id
ORDER BY r.reg_date ASC
LIMIT %(limit)s OFFSET %(offset)s;
