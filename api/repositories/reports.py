from google.cloud import bigquery

class Reports:

    @staticmethod
    def hiries_by_quarter(year):

        query = f"""
        WITH hires AS (
            SELECT
                h.id,
                h.name,
                h.hire_datetime,
                h.department_id,
                h.job_id
            FROM
                `poc-globant-data.raw.hired_employees` h
            WHERE
                    SUBSTR(h.hire_datetime, 1, 4) = '{year}'
            ),
            quarter_counts AS (
            SELECT
                j.job,
                d.department,
                SUBSTR(h.hire_datetime, 6, 2) AS quarter,
                COUNT(h.id) AS hire_count
            FROM
                hires h
                JOIN  `poc-globant-data.raw.jobs` j ON h.job_id = j.id
                JOIN  `poc-globant-data.raw.departments` d ON h.department_id = d.id
            GROUP BY
                j.job, d.department, quarter
            )
            SELECT
            department,
            job,
            SUM(IF(quarter = '01', hire_count, 0)) AS Q1,
            SUM(IF(quarter = '04', hire_count, 0)) AS Q2,
            SUM(IF(quarter = '07', hire_count, 0)) AS Q3,
            SUM(IF(quarter = '10', hire_count, 0)) AS Q4
            FROM
            quarter_counts
            GROUP BY
            department, job
            ORDER BY
            department, job;
        """
        
        formatted_query = query.replace('\n', ' ').replace('  ', ' ').replace('\t', ' ')
       
        
        client = bigquery.Client()
        job_config = bigquery.QueryJobConfig(use_legacy_sql=False)
        query_job = client.query(formatted_query, job_config=job_config)

        result = query_job.result()

        return result
    
    @staticmethod
    def departments_by_hires(year):

        query = f"""
        WITH department_hires AS (
          SELECT
            d.id AS department_id,
            d.department,
            COUNT(h.id) AS hire_count
          FROM
            `poc-globant-data.raw.hired_employees` h
            JOIN `poc-globant-data.raw.departments` d ON h.department_id = d.id
          WHERE
            SUBSTR(h.hire_datetime, 1, 4) = '{year}'
          GROUP BY
            department_id, d.department
        ),
        average_hires AS (
          SELECT
            AVG(hire_count) AS avg_hire_count
          FROM
            department_hires
        )
        SELECT
          dh.department_id as id,
          dh.department,
          dh.hire_count
        FROM
          department_hires dh
          JOIN average_hires ah ON dh.hire_count > ah.avg_hire_count
        ORDER BY
          dh.hire_count DESC;
        """

        formatted_query = query.replace('\n', ' ').replace('  ', ' ').replace('\t', ' ')
       
        
        client = bigquery.Client()
        job_config = bigquery.QueryJobConfig(use_legacy_sql=False)
        query_job = client.query(formatted_query, job_config=job_config)

        result = query_job.result()

        return result