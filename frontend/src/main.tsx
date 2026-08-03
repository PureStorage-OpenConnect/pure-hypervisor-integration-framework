import React from "react";
import ReactDOM from "react-dom/client";
import { createBrowserRouter, RouterProvider } from "react-router-dom";
import App from "./App";
import WizardPage from "./pages/WizardPage";
import ArraysPage from "./pages/ArraysPage";
import ApiKeysPage from "./pages/ApiKeysPage";
import HypervisorsPage from "./pages/HypervisorsPage";
import OperationsPage from "./pages/OperationsPage";
import MigrationPage from "./pages/MigrationPage";
import JobsPage from "./pages/JobsPage";
import SettingsPage from "./pages/SettingsPage";
import VspherePluginPage from "./pages/VspherePluginPage";
import "./styles.css";

const router = createBrowserRouter([
  {
    path: "/",
    element: <App />,
    children: [
      { index: true, element: <WizardPage /> },
      { path: "wizard", element: <WizardPage /> },
      { path: "arrays", element: <ArraysPage /> },
      { path: "api-keys", element: <ApiKeysPage /> },
      { path: "hypervisors", element: <HypervisorsPage /> },
      { path: "operations", element: <OperationsPage /> },
      { path: "operations/:hypervisorId", element: <OperationsPage /> },
      { path: "migrate", element: <MigrationPage /> },
      { path: "jobs", element: <JobsPage /> },
      { path: "settings", element: <SettingsPage /> },
      { path: "vsphere-plugin", element: <VspherePluginPage /> },
    ],
  },
]);

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <RouterProvider router={router} />
  </React.StrictMode>,
);
