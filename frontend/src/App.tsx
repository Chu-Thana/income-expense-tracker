import { useEffect, useState } from "react";
import type { FormEvent } from "react";
import "./App.css";

const API_URL = "http://127.0.0.1:8000";
const USER_ID = 1; // Mock user, no authentication
const DEMO_MODE = import.meta.env.VITE_DEMO_MODE === "true";
const DEMO_STORAGE_KEY = "income-expense-public-demo-transactions-v1";

type Transaction = {
  transaction_id: number;
  user_id: number;
  transaction_date: string;
  transaction_type: "income" | "expense";
  category: string;
  amount: number;
  description: string | null;
};

type MonthlySummary = {
  month: string;
  total_income: number;
  total_expense: number;
  net_amount: number;
};

function App() {
  const [transactions, setTransactions] = useState<Transaction[]>(() => {
    if (!DEMO_MODE) return [];
    try {
      const saved: unknown = JSON.parse(localStorage.getItem(DEMO_STORAGE_KEY) ?? "[]");
      if (!Array.isArray(saved)) return [];
      return saved.filter((item): item is Transaction =>
        typeof item === "object" && item !== null &&
        typeof item.transaction_id === "number" &&
        typeof item.transaction_date === "string" &&
        (item.transaction_type === "income" || item.transaction_type === "expense") &&
        typeof item.category === "string" &&
        typeof item.amount === "number" && Number.isFinite(item.amount)
      );
    } catch { return []; }
  });
  const [monthly, setMonthly] = useState<MonthlySummary[]>([]);
  const [demoBaseline, setDemoBaseline] = useState<MonthlySummary[]>([]);
  const [selectedMonth, setSelectedMonth] = useState<string | null>(null);
  const [date, setDate] = useState("2026-10-31");
  const [type, setType] = useState<"income" | "expense">("expense");
  const [category, setCategory] = useState("Food");
  const [amount, setAmount] = useState("");
  const [description, setDescription] = useState("");
  const [message, setMessage] = useState("");
  const [analyticsError, setAnalyticsError] = useState("");
  const [isUpdatingAnalytics, setIsUpdatingAnalytics] = useState(false);
  const [pipelineMessage, setPipelineMessage] = useState("");

  async function loadTransactions() {
    if (DEMO_MODE) return;
    const response = await fetch(
      `${API_URL}/transactions?user_id=${USER_ID}`
    );

    if (!response.ok) {
      throw new Error("Failed to load transactions");
    }

    setTransactions(await response.json());
  }

  async function loadAnalytics() {
    try {
      setAnalyticsError("");
      const response = await fetch(
        DEMO_MODE
          ? `${import.meta.env.BASE_URL}demo/monthly_summary.json`
          : `${API_URL}/analytics/monthly?user_id=${USER_ID}`
      );

      if (!response.ok) {
        throw new Error("Redshift analytics request failed");
      }

      const data: unknown = await response.json();
      if (!Array.isArray(data) || !data.every((item) =>
        item && typeof item.month === "string" &&
        typeof item.total_income === "number" &&
        typeof item.total_expense === "number" &&
        typeof item.net_amount === "number"
      )) {
        throw new Error("Invalid analytics data");
      }
      const summaries = data as MonthlySummary[];
      if (DEMO_MODE) setDemoBaseline(summaries);
      else setMonthly(summaries);
      return true;
    } catch {
      setAnalyticsError(DEMO_MODE ? "Cannot load demo data" : "Cannot load Redshift analytics");
      return false;
    }
  }

  async function handleUpdateAnalytics() {
    if (DEMO_MODE || isUpdatingAnalytics) return;

    setIsUpdatingAnalytics(true);
    setPipelineMessage("Starting Airflow pipeline...");

    try {
      const triggerResponse = await fetch(`${API_URL}/pipeline/trigger`, {
        method: "POST",
      });
      if (!triggerResponse.ok) {
        throw new Error("Could not start Airflow pipeline");
      }

      const run: { dag_run_id: string; state: string } =
        await triggerResponse.json();
      const encodedId = encodeURIComponent(run.dag_run_id);

      // Poll the same DAG run; never trigger a new run while checking its status.
      for (let attempt = 0; attempt < 60; attempt += 1) {
        const statusResponse = await fetch(
          `${API_URL}/pipeline/status/${encodedId}`
        );
        if (!statusResponse.ok) {
          throw new Error("Could not check Airflow pipeline status");
        }

        const result: { state: string } = await statusResponse.json();
        if (result.state === "success") {
          setPipelineMessage("Pipeline completed. Refreshing analytics...");
          const refreshed = await loadAnalytics();
          if (!refreshed) {
            throw new Error("Pipeline completed, but analytics could not be refreshed");
          }
          setPipelineMessage("Analytics updated successfully!");
          return;
        }
        if (result.state === "failed") {
          throw new Error("Airflow pipeline failed. Check task logs.");
        }
        if (!["queued", "running"].includes(result.state)) {
          throw new Error(`Unexpected pipeline state: ${result.state}`);
        }

        setPipelineMessage(`Airflow pipeline: ${result.state}...`);
        await new Promise<void>((resolve) => setTimeout(resolve, 3000));
      }

      throw new Error("Status check timed out. Check Airflow before retrying.");
    } catch (error) {
      setPipelineMessage(
        error instanceof Error ? error.message : "Failed to update analytics"
      );
    } finally {
      setIsUpdatingAnalytics(false);
    }
  }

  useEffect(() => {
    loadTransactions().catch(() => {
      setMessage("Cannot connect to FastAPI");
    });
    void loadAnalytics();
  }, []);

  useEffect(() => {
    if (!DEMO_MODE || demoBaseline.length === 0) return;

    const updatedMonthly = demoBaseline.map((item) => {
      const monthTransactions = transactions.filter(
        (tx) => tx.transaction_date.slice(0, 7) === item.month
      );

      const addedIncome = monthTransactions
        .filter((tx) => tx.transaction_type === "income")
        .reduce((sum, tx) => sum + tx.amount, 0);

      const addedExpense = monthTransactions
        .filter((tx) => tx.transaction_type === "expense")
        .reduce((sum, tx) => sum + tx.amount, 0);

      const totalIncome = item.total_income + addedIncome;
      const totalExpense = item.total_expense + addedExpense;

      return {
        ...item,
        total_income: totalIncome,
        total_expense: totalExpense,
        net_amount: totalIncome - totalExpense,
      };
    });

    setMonthly(updatedMonthly);
  }, [demoBaseline, transactions]);

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setMessage("");

    if (DEMO_MODE) {
      const value = Number(amount);
      if (!Number.isFinite(value) || value <= 0) {
        setMessage("Please enter an amount greater than zero.");
        return;
      }
      if (!demoBaseline.some((item) => item.month === date.slice(0, 7))) {
        setMessage("Demo supports dates in the displayed six months only.");
        return;
      }
      const newTransaction: Transaction = {
        transaction_id: Date.now(),
        user_id: USER_ID,
        transaction_date: date,
        transaction_type: type,
        category,
        amount: value,
        description: description || null,
      };
      const updated = [newTransaction, ...transactions];
      try {
        localStorage.setItem(DEMO_STORAGE_KEY, JSON.stringify(updated));
      } catch {
        setMessage("Browser storage unavailable. Changes will last until refresh.");
      }
      setTransactions(updated);
      setSelectedMonth(date.slice(0, 7));
      setAmount("");
      setDescription("");
      setMessage("Demo transaction added. Chart updated locally (no real pipeline run).");
      return;
    }

    try {
      const response = await fetch(`${API_URL}/transactions`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          user_id: USER_ID,
          transaction_date: date,
          transaction_type: type,
          category,
          amount: Number(amount),
          description,
        }),
      });

      if (!response.ok) {
        throw new Error("Failed to save transaction");
      }

      setAmount("");
      setDescription("");
      setMessage("Transaction saved successfully!");
      await loadTransactions();
    } catch {
      setMessage("Failed to save or refresh transaction");
    }
  }

  const latest = monthly[monthly.length - 1];
  const selectedSummary =
    monthly.find((item) => item.month === selectedMonth) ?? latest;

  const monthlyWithBalance = [...monthly]
  .sort((a, b) => a.month.localeCompare(b.month))
  .reduce<
    (MonthlySummary & { running_balance: number })[]
  >((result, item) => {
    const previousBalance =
      result.length > 0
        ? result[result.length - 1].running_balance
        : 0;

    result.push({
      ...item,
      running_balance: previousBalance + item.net_amount,
    });

    return result;
  }, []);

  const selectedBalance =
  monthlyWithBalance.find(
    (item) => item.month === selectedSummary?.month
  )?.running_balance ?? 0;

  const chartMax = Math.max(
    30000,
    ...monthly.flatMap((item) => [item.total_income, item.total_expense])
  );
  const yAxisTicks = [
    chartMax,
    Math.round((chartMax * 2) / 3),
    Math.round(chartMax / 3),
    0,
  ];

  const formatMoney = (value: number) =>
    new Intl.NumberFormat("th-TH", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(value);

  const formatThaiMonthLabel = (month: string) => {
    const [year, mm] = month.split("-");
    const monthNames = [
      "ม.ค.", "ก.พ.", "มี.ค.", "เม.ย.", "พ.ค.", "มิ.ย.",
      "ก.ค.", "ส.ค.", "ก.ย.", "ต.ค.", "พ.ย.", "ธ.ค.",
    ];
    const thaiYearShort = String(Number(year) + 543).slice(-2);
    return {
      monthText: monthNames[Number(mm) - 1],
      yearText: thaiYearShort,
    };
  };

  return (
    <div className="container">
      <h1>Income & Expense Tracker</h1>

      <section className="analytics analytics-card">
        <div className="analytics-top">
          <div>
            <h2>สรุปรายรับรายจ่าย</h2>
            <p className="analytics-subtitle">
              เปรียบเทียบรายรับรายจ่ายย้อนหลัง 6 เดือน
            </p>
          </div>
          {!DEMO_MODE && <div style={{ display: "flex", gap: "8px", flexWrap: "wrap" }}>
            <button
              type="button"
              onClick={handleUpdateAnalytics}
              disabled={isUpdatingAnalytics}
            >
              {isUpdatingAnalytics ? "Processing..." : "Update Analytics"}
            </button>
            <button
              type="button"
              onClick={() => { void loadAnalytics(); }}
              disabled={isUpdatingAnalytics}
            >
              Refresh Analytics
            </button>
          </div>}
        </div>

        {DEMO_MODE && (
          <p className="analytics-note" role="note">
            Interactive Public Demo · Mock data · Transactions are saved only in this browser.
            This page does not connect to PostgreSQL, Airflow or AWS.
          </p>
        )}
        {pipelineMessage && <p role="status">{pipelineMessage}</p>}
        {analyticsError && <p className="error">{analyticsError}</p>}

        {monthly.length > 0 && latest && (
          <>
            <div className="ie-chart-legend">
              <span>
                <span className="legend-dot income-dot"></span> เงินเข้า
              </span>
              <span>
                <span className="legend-dot expense-dot"></span> เงินออก
              </span>
            </div>

            <div className="ie-chart-layout">
              <div className="ie-y-axis">
                {yAxisTicks.map((tick, index) => (
                  <span
                    key={index}
                    className="ie-y-label"
                    style={{
                      top: `${(index / (yAxisTicks.length - 1)) * 100}%`,
                    }}
                  >
                    {formatMoney(tick)}
                  </span>
                ))}
              </div>

              <div className="ie-chart-content">
                <div className="ie-plot">
                  {yAxisTicks.map((_, index) => (
                    <div
                      key={index}
                      className="ie-grid-line"
                      style={{
                        top: `${(index / (yAxisTicks.length - 1)) * 100}%`,
                      }}
                    />
                  ))}

                  <div className="ie-bars">
                    {monthly.map((item) => (
                      <div
                        className={`ie-bar-group ${
                            selectedSummary?.month === item.month ? "selected" : ""
                        }`}
                        key={item.month}
                        onClick={() => setSelectedMonth(item.month)}
                        title={`คลิกเพื่อดูข้อมูลเดือน ${item.month}`}
                        >
                        <div
                          className="ie-bar ie-income"
                          style={{
                            height: `${(item.total_income / chartMax) * 100}%`,
                          }}
                          title={`เงินเข้า ${formatMoney(item.total_income)} บาท`}
                        />
                        <div
                          className="ie-bar ie-expense"
                          style={{
                            height: `${(item.total_expense / chartMax) * 100}%`,
                          }}
                          title={`เงินออก ${formatMoney(item.total_expense)} บาท`}
                        />
                      </div>
                    ))}
                  </div>
                </div>

                <div className="ie-x-axis">
                  {monthly.map((item) => {
                    const label = formatThaiMonthLabel(item.month);
                    return (
                      <div className="ie-x-label" key={item.month}>
                        <span>{label.monthText}</span>
                        <span>{label.yearText}</span>
                      </div>
                    );
                  })}
                </div>
              </div>
            </div>

            <div className="summary-bottom">
              <div className="summary-card income-summary">
                <p>
                  เงินเข้าเดือน{" "}
                  {formatThaiMonthLabel(selectedSummary.month).monthText}{" "}
                  {formatThaiMonthLabel(selectedSummary.month).yearText}
                </p>
                <h3>{formatMoney(selectedSummary.total_income)}</h3>
              </div>

              <div className="summary-card expense-summary">
                <p>
                  เงินออกเดือน{" "}
                  {formatThaiMonthLabel(selectedSummary.month).monthText}{" "}
                  {formatThaiMonthLabel(selectedSummary.month).yearText}
                </p>
                <h3>{formatMoney(selectedSummary.total_expense)}</h3>
              </div>

              <div className="summary-card net-summary">
                <p>เงินสุทธิประจำเดือน</p>
                <h3>{formatMoney(selectedSummary.net_amount)}</h3>
              </div>

              <div className="summary-card balance-summary">
                <p>ยอดสุทธิสะสมถึงเดือนที่เลือก</p>
                <h3>{formatMoney(selectedBalance)}</h3>
              </div>
            </div>

            <p className="analytics-note">
              {DEMO_MODE
                ? "Public Demo: กราฟคำนวณจากข้อมูลจำลองใน Browser ไม่ได้เรียก Airflow หรือ Redshift"
                : "ข้อมูล Analytics จะอัปเดตหลัง Airflow batch pipeline ทำงานเสร็จ"}
            </p>
          </>
        )}

        {!latest && !analyticsError && <p>No analytics data available.</p>}
      </section>

      <form onSubmit={handleSubmit}>
        <h2>Add Transaction</h2>
        <label>Date</label>
        <input
          type="date"
          value={date}
          onChange={(e) => setDate(e.target.value)}
          min={DEMO_MODE ? demoBaseline[0]?.month + "-01" : undefined}
          max={DEMO_MODE ? demoBaseline[demoBaseline.length - 1]?.month + "-31" : undefined}
          required
        />

        <label>Type</label>
        <select
          value={type}
          onChange={(e) =>
            setType(e.target.value as "income" | "expense")
          }
        >
          <option value="income">Income</option>
          <option value="expense">Expense</option>
        </select>

        <label>Category</label>
        <input
          value={category}
          onChange={(e) => setCategory(e.target.value)}
          required
        />

        <label>Amount (THB)</label>
        <input
          type="number"
          min="0.01"
          step="0.01"
          value={amount}
          onChange={(e) => setAmount(e.target.value)}
          required
        />

        <label>Description</label>
        <input
          value={description}
          onChange={(e) => setDescription(e.target.value)}
        />
        <button type="submit">Save Transaction</button>
      </form>

      {message && <p role="status">{message}</p>}
      <h2>{DEMO_MODE ? "Demo Transaction History (this browser only)" : "Transaction History"}</h2>
      {DEMO_MODE && (
        <div style={{ marginBottom: "12px" }}>
          <button
            type="button"
            onClick={() => {
              localStorage.removeItem(DEMO_STORAGE_KEY);
              setTransactions([]);
              setSelectedMonth(null);
              setMessage("Demo reset to original snapshot.");
            }}
          >
            Reset Demo Data
          </button>
        </div>
      )}
      <div className="table-wrapper">
        <table>
          <thead>
            <tr>
              <th>Date</th>
              <th>Type</th>
              <th>Category</th>
              <th>Amount</th>
            </tr>
          </thead>
          <tbody>
            {DEMO_MODE && transactions.length === 0 && (
              <tr><td colSpan={4}>No demo transactions yet. Add one above to test.</td></tr>
            )}
            {transactions.map((tx) => (
              <tr key={tx.transaction_id}>
                <td>{tx.transaction_date}</td>
                <td>{tx.transaction_type}</td>
                <td>{tx.category}</td>
                <td>{formatMoney(Number(tx.amount))}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default App;
