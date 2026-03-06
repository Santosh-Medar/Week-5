import csv
import json
import sqlite3
import logging
from datetime import datetime

DB_NAME = "payroll.db"
EMPLOYEE_FILE = "zenvy_employees.csv"
ATTENDANCE_FILE = "zenvy_attendance.csv"
RULES_FILE = "rules.json"

# -------------------------------
# Logging Setup (Audit Framework)
# -------------------------------
logging.basicConfig(
    filename="audit.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


# -------------------------------
# Load Payroll Rules
# -------------------------------
def load_rules():
    with open(RULES_FILE) as f:
        return json.load(f)


# -------------------------------
# Load Employees
# -------------------------------
def load_employees():
    employees = {}

    with open(EMPLOYEE_FILE) as file:
        reader = csv.DictReader(file)

        for row in reader:
            emp_id = row["employee_id"].strip()

            if not emp_id:
                continue

            employees[emp_id] = {
                "name": row["employee_name"],
                "base_salary": int(row["base_salary"]) if row["base_salary"] else 0,
                "department": row["department"],
                "designation": row["designation"]
            }

    return employees


# -------------------------------
# Load Attendance
# -------------------------------
def load_attendance():
    attendance = {}

    with open(ATTENDANCE_FILE) as file:
        reader = csv.DictReader(file)

        for row in reader:
            emp_id = row["employee_id"].strip()

            if not emp_id:
                continue

            overtime = int(float(row["overtime_hours"])) if row["overtime_hours"] else 0

            attendance[emp_id] = overtime

    return attendance


# -------------------------------
# Payroll Engine
# -------------------------------
class PayrollEngine:

    def __init__(self, rules):
        self.rules = rules

    def calculate(self, base_salary, overtime):

        overtime_pay = overtime * self.rules["overtime_rate"]

        gross = base_salary + overtime_pay

        tax = gross * (self.rules["tax_percentage"] / 100)

        pf = gross * (self.rules["pf_percentage"] / 100)

        net = gross - (tax + pf)

        return {
            "gross": round(gross, 2),
            "tax": round(tax, 2),
            "pf": round(pf, 2),
            "net": round(net, 2)
        }


# -------------------------------
# Database Setup
# -------------------------------
def setup_database(cursor):

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS payroll_status(
            month TEXT PRIMARY KEY,
            status TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS payroll_records(
            employee_id TEXT,
            month TEXT,
            gross REAL,
            tax REAL,
            pf REAL,
            net REAL,
            processed_at TEXT
        )
    """)


# -------------------------------
# Batch Processor
# -------------------------------
def process_batch(batch, employees, attendance, engine, cursor, month):

    for emp_id in batch:

        emp = employees[emp_id]

        base_salary = emp["base_salary"]

        overtime = attendance.get(emp_id, 0)

        result = engine.calculate(base_salary, overtime)

        cursor.execute("""
            INSERT INTO payroll_records
            VALUES (?,?,?,?,?,?,?)
        """, (
            emp_id,
            month,
            result["gross"],
            result["tax"],
            result["pf"],
            result["net"],
            datetime.now()
        ))

        print(f"{emp_id} | Net Salary: {result['net']}")

        logging.info(
            f"{emp_id} | Gross:{result['gross']} | Tax:{result['tax']} | PF:{result['pf']} | Net:{result['net']}"
        )


# -------------------------------
# Run Payroll (Transaction Safe)
# -------------------------------
def run_payroll(month, batch_size=5):

    conn = sqlite3.connect(DB_NAME)

    cursor = conn.cursor()

    setup_database(cursor)

    cursor.execute(
        "SELECT status FROM payroll_status WHERE month=?",
        (month,)
    )

    record = cursor.fetchone()

    if record and record[0] == "LOCKED":
        print("Payroll already processed for", month)
        return

    try:

        conn.execute("BEGIN")

        rules = load_rules()

        employees = load_employees()

        attendance = load_attendance()

        engine = PayrollEngine(rules)

        employee_ids = list(employees.keys())

        # Batch Processing
        for i in range(0, len(employee_ids), batch_size):

            batch = employee_ids[i:i + batch_size]

            process_batch(
                batch,
                employees,
                attendance,
                engine,
                cursor,
                month
            )

        cursor.execute(
            "INSERT OR REPLACE INTO payroll_status VALUES (?,?)",
            (month, "LOCKED")
        )

        conn.commit()

        print("\nPayroll Completed Successfully")
        logging.info(f"Payroll LOCKED for {month}")

    except Exception as e:

        conn.rollback()

        print("Error occurred → Transaction Rolled Back")

        logging.error(f"Payroll failed: {str(e)}")

    finally:

        conn.close()


# -------------------------------
# Execute
# -------------------------------
run_payroll("March")