import { BrowserRouter, Navigate, Route, Routes } from 'react-router-dom'
import { useAuthStore } from '@/store/auth'
import Layout from '@/components/layout/Layout'
import LoginPage from '@/pages/LoginPage'
import RnPPage from '@/pages/RnPPage'
import SalesPage from '@/pages/SalesPage'
import OtsifrovkaPage from '@/pages/OtsifrovkaPage'
import SettingsPage from '@/pages/SettingsPage'
import OPiUPage from '@/pages/OPiUPage'
import SverkaPage from '@/pages/SverkaPage'
import ElasticityPage from '@/pages/ElasticityPage'
import DDSPage from '@/pages/DDSPage'
import PaymentCalendarPage from '@/pages/PaymentCalendarPage'
import BalanceSheetPage from '@/pages/BalanceSheetPage'
import JournalPage from '@/pages/JournalPage'
import AnalyticsPage from '@/pages/AnalyticsPage'

function PrivateRoute({ children }: { children: React.ReactNode }) {
  const token = useAuthStore((s) => s.token)
  return token ? <>{children}</> : <Navigate to="/login" replace />
}

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/login" element={<LoginPage />} />
        <Route
          path="/"
          element={
            <PrivateRoute>
              <Layout />
            </PrivateRoute>
          }
        >
          <Route index element={<Navigate to="/rnp" replace />} />
          <Route path="analytics" element={<AnalyticsPage />} />
          <Route path="rnp" element={<RnPPage />} />
          <Route path="otsifrovka" element={<OtsifrovkaPage />} />
          <Route path="journal" element={<JournalPage />} />
          <Route path="opiu" element={<OPiUPage />} />
          <Route path="dds" element={<DDSPage />} />
          <Route path="payment-calendar" element={<PaymentCalendarPage />} />
          <Route path="balance-sheet" element={<BalanceSheetPage />} />
          <Route path="sales" element={<SalesPage />} />
          <Route path="sverka" element={<SverkaPage />} />
          <Route path="elasticity" element={<ElasticityPage />} />
          <Route path="settings" element={<SettingsPage />} />
        </Route>
        <Route path="*" element={<Navigate to="/rnp" replace />} />
      </Routes>
    </BrowserRouter>
  )
}
