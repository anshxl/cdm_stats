import { Navigate, Route, Routes, useLocation } from 'react-router'
import { AppShell } from '@/components/AppShell'
import EloPage from '@/pages/EloPage'
import H2HPage from '@/pages/H2HPage'
import ScrimsPage from '@/pages/ScrimsPage'
import TeamPage from '@/pages/TeamPage'
import TrainingPage from '@/pages/TrainingPage'

function ToTeam() {
  const { search } = useLocation()
  return <Navigate to={{ pathname: '/team', search }} replace />
}

export default function App() {
  return (
    <Routes>
      <Route element={<AppShell />}>
        <Route path="/team" element={<TeamPage />} />
        <Route path="/h2h" element={<H2HPage />} />
        <Route path="/scrims" element={<ScrimsPage />} />
        <Route path="/elo" element={<EloPage />} />
        <Route path="/training" element={<TrainingPage />} />
        <Route path="*" element={<ToTeam />} />
      </Route>
    </Routes>
  )
}
