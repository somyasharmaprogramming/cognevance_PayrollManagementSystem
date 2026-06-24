import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

// ==========================================
// 1. CUSTOM EXCEPTIONS (Exception Handling)
// ==========================================
class DatabaseException extends Exception {
    public DatabaseException(String message, Throwable cause) {
        super(message, cause);
    }
}

class InvalidDataException extends Exception {
    public InvalidDataException(String message) {
        super(message);
    }
}

// ==========================================
// 2. MODEL CLASSES
// ==========================================
class Employee {
    private int id;
    private String name;
    private String role;
    private double baseSalary;

    public Employee(int id, String name, String role, double baseSalary) {
        this.id = id;
        this.name = name;
        this.role = role;
        this.baseSalary = baseSalary;
    }

    public int getId() { return id; }
    public String getName() { return name; }
    public String getRole() { return role; }
    public double getBaseSalary() { return baseSalary; }
}

// ==========================================
// 3. DATABASE MANAGER (JDBC & MySQL/H2)
// ==========================================
class DatabaseManager {
    // Using H2 In-Memory Database mimicking MySQL for instant execution
    private static final String URL = "jdbc:h2:mem:payroll_db;MODE=MySQL;DB_CLOSE_DELAY=-1";
    private static final String USER = "sa";
    private static final String PASSWORD = "";

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(URL, USER, PASSWORD);
    }

    public static void initializeDatabase() throws DatabaseException {
        String createEmployees = "CREATE TABLE IF NOT EXISTS employees (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100), role VARCHAR(50), base_salary DOUBLE);";
        String createAttendance = "CREATE TABLE IF NOT EXISTS attendance (id INT AUTO_INCREMENT PRIMARY KEY, employee_id INT, days_present INT, total_working_days INT);";
        String createPayroll = "CREATE TABLE IF NOT EXISTS payroll (id INT AUTO_INCREMENT PRIMARY KEY, employee_id INT, net_salary DOUBLE, pay_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP);";

        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute(createEmployees);
            stmt.execute(createAttendance);
            stmt.execute(createPayroll);
        } catch (SQLException e) {
            throw new DatabaseException("Failed to initialize database tables.", e);
        }
    }
}

// ==========================================
// 4. CORE MODULES & SERVICES
// ==========================================
class PayrollService {

    // Task 1: Employee Registration
    public void registerEmployee(String name, String role, double baseSalary) throws DatabaseException, InvalidDataException {
        if (name == null || name.trim().isEmpty() || baseSalary <= 0) {
            throw new InvalidDataException("Invalid Employee Details Provided.");
        }
        String sql = "INSERT INTO employees (name, role, base_salary) VALUES (?, ?, ?)";
        try (Connection conn = DatabaseManager.getConnection(); PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, name);
            pstmt.setString(2, role);
            pstmt.setDouble(3, baseSalary);
            pstmt.executeUpdate();
            System.out.println("🎉 Employee registered successfully: " + name);
        } catch (SQLException e) {
            throw new DatabaseException("Error saving employee record.", e);
        }
    }

    // Task 1: Attendance/Salary Setup
    public void recordAttendance(int employeeId, int daysPresent, int totalDays) throws DatabaseException, InvalidDataException {
        if (daysPresent > totalDays || daysPresent < 0) {
            throw new InvalidDataException("Days present cannot exceed total working days or be negative.");
        }
        String sql = "INSERT INTO attendance (employee_id, days_present, total_working_days) VALUES (?, ?, ?)";
        try (Connection conn = DatabaseManager.getConnection(); PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, employeeId);
            pstmt.setInt(2, daysPresent);
            pstmt.setInt(3, totalDays);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new DatabaseException("Error logging attendance.", e);
        }
    }

    // Task 3 & 4: Concurrent Payroll Batch Generation (Multithreading)
    public void processBulkPayroll() throws DatabaseException {
        List<Employee> employees = new ArrayList<>();
        String query = "SELECT * FROM employees";

        try (Connection conn = DatabaseManager.getConnection(); 
             Statement stmt = conn.createStatement(); 
             ResultSet rs = stmt.executeQuery(query)) {
            
            while (rs.next()) {
                employees.add(new Employee(
                    rs.getInt("id"),
                    rs.getString("name"),
                    rs.getString("role"),
                    rs.getDouble("base_salary")
                ));
            }
        } catch (SQLException e) {
            throw new DatabaseException("Error fetching employees for payroll processing.", e);
        }

        // Thread pool to handle concurrent calculations
        ExecutorService executor = Executors.newFixedThreadPool(3);

        System.out.println("\n🚀 Initiating Concurrent Payroll Processing...");
        for (Employee emp : employees) {
            executor.execute(() -> {
                try {
                    processSinglePayroll(emp);
                } catch (Exception e) {
                    System.err.println("❌ Thread execution error for Employee ID " + emp.getId() + ": " + e.getMessage());
                }
            });
        }

        executor.shutdown();
        try {
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Payroll thread execution interrupted.");
        }
    }

    // Task 3: Salary Calculation Mechanics
    private void processSinglePayroll(Employee emp) throws SQLException {
        String attendanceQuery = "SELECT days_present, total_working_days FROM attendance WHERE employee_id = ? ORDER BY id DESC LIMIT 1";
        int daysPresent = 22; // Defaults if record missing
        int totalDays = 22;

        try (Connection conn = DatabaseManager.getConnection(); PreparedStatement pstmt = conn.prepareStatement(attendanceQuery)) {
            pstmt.setInt(1, emp.getId());
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    daysPresent = rs.getInt("days_present");
                    totalDays = rs.getInt("total_working_days");
                }
            }
        }

        // Formula: Net Salary = (Base Salary / Total Days) * Days Present
        double calculatedNet = (emp.getBaseSalary() / totalDays) * daysPresent;

        // Insert calculation to DB
        String insertPayroll = "INSERT INTO payroll (employee_id, net_salary) VALUES (?, ?)";
        try (Connection conn = DatabaseManager.getConnection(); PreparedStatement pstmt = conn.prepareStatement(insertPayroll)) {
            pstmt.setInt(1, emp.getId());
            pstmt.setDouble(2, calculatedNet);
            pstmt.executeUpdate();
            System.out.println("✅ Processed payroll for: " + emp.getName() + " | Net: $" + String.format("%.2f", calculatedNet));
        }
    }

    // Task 5: Reporting System
    public void generatePayrollReport() throws DatabaseException {
        String reportQuery = "SELECT e.id, e.name, e.role, a.days_present, a.total_working_days, p.net_salary, p.pay_date " +
                             "FROM employees e " +
                             "JOIN attendance a ON e.id = a.employee_id " +
                             "JOIN payroll p ON e.id = p.employee_id";

        System.out.println("\n==========================================================================================");
        System.out.println("                                  PAYROLL & ATTENDANCE REPORT                             ");
        System.out.println("==========================================================================================");
        System.out.printf("%-5s | %-15s | %-15s | %-10s | %-12s | %-15s\n", "ID", "Name", "Role", "Attd.", "Net Salary", "Pay Date");
        System.out.println("------------------------------------------------------------------------------------------");

        try (Connection conn = DatabaseManager.getConnection(); 
             Statement stmt = conn.createStatement(); 
             ResultSet rs = stmt.executeQuery(reportQuery)) {
            
            while (rs.next()) {
                System.out.printf("%-5d | %-15s | %-15s | %d/%d days  | $%-11.2f | %s\n",
                    rs.getInt("id"),
                    rs.getString("name"),
                    rs.getString("role"),
                    rs.getInt("days_present"),
                    rs.getInt("total_working_days"),
                    rs.getDouble("net_salary"),
                    rs.getTimestamp("pay_date").toString()
                );
            }
            System.out.println("==========================================================================================");
        } catch (SQLException e) {
            throw new DatabaseException("Error rendering payroll summary report.", e);
        }
    }
}

// ==========================================
// 5. APPLICATION RUNNER / ENTRY POINT
// ==========================================
public class PayrollManagementSystem {
    public static void main(String[] args) {
        System.out.println("⚙️ Bootstrapping Payroll Engine...");
        PayrollService payrollService = new PayrollService();

        try {
            // Step 1: Init Database Tables
            DatabaseManager.initializeDatabase();

            // Step 2: Seed Employee Registrations (Task 1)
            System.out.println("\n--- Seeding Employee Records ---");
            payrollService.registerEmployee("Alice Smith", "Software Engineer", 8500.00);
            payrollService.registerEmployee("Bob Jones", "UX Designer", 7200.00);
            payrollService.registerEmployee("Charlie Brown", "Product Manager", 9500.00);

            // Step 3: Seed Attendance Records
            payrollService.recordAttendance(1, 22, 22); // Alice: Full Attendance
            payrollService.recordAttendance(2, 18, 22); // Bob: Missed 4 days
            payrollService.recordAttendance(3, 20, 22); // Charlie: Missed 2 days

            // Step 4: Run Concurrent Calculation (Task 3 & 4)
            payrollService.processBulkPayroll();

            // Step 5: Output Generated Reports (Task 5)
            payrollService.generatePayrollReport();

        } catch (DatabaseException e) {
            System.err.println("❌ Database Core Exception: " + e.getMessage());
            e.printStackTrace();
        } catch (InvalidDataException e) {
            System.err.println("⚠️ Validation Error: " + e.getMessage());
        }
    }
}