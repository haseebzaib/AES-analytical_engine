document.addEventListener("DOMContentLoaded", () => {
    const dashboardShell = document.querySelector("[data-dashboard-shell]");
    const sidebarToggle = document.querySelector("[data-sidebar-toggle]");
    const sidebarStorageKey = "metacrust.sidebar.collapsed";

    if (dashboardShell && sidebarToggle) {
        const applySidebarState = (collapsed) => {
            dashboardShell.classList.toggle("is-sidebar-collapsed", collapsed);
            sidebarToggle.setAttribute("aria-expanded", collapsed ? "false" : "true");
        };

        const storedSidebarState = window.localStorage.getItem(sidebarStorageKey);
        applySidebarState(storedSidebarState === "true");

        sidebarToggle.addEventListener("click", () => {
            const collapsed = !dashboardShell.classList.contains("is-sidebar-collapsed");
            applySidebarState(collapsed);
            window.localStorage.setItem(sidebarStorageKey, collapsed ? "true" : "false");
        });
    }

    const overviewShell = document.querySelector("[data-overview-shell]");
    if (overviewShell) {
        const chipGateway = overviewShell.querySelector('[data-overview-chip="gateway"]');
        const chipPrimary = overviewShell.querySelector('[data-overview-chip="primary-link"]');
        const chipWireless = overviewShell.querySelector('[data-overview-chip="wireless"]');
        const led = overviewShell.querySelector("[data-overview-led]");
        const ethLink = overviewShell.querySelector("[data-overview-eth-link]");
        const wifiLink = overviewShell.querySelector("[data-overview-wifi-link]");
        const ethPort = overviewShell.querySelector("[data-overview-eth-port]");
        const wifiPort = overviewShell.querySelector("[data-overview-wifi-port]");
        const ethernetItem = overviewShell.querySelector('[data-overview-item="ethernet"]');
        const wifiItem = overviewShell.querySelector('[data-overview-item="wi-fi"]');
        const systemMetricsShell = document.querySelector("[data-system-metrics-shell]");
        const systemCpuSummary = document.querySelector("[data-system-cpu-summary]");
        const systemMemorySummary = document.querySelector("[data-system-memory-summary]");
        const systemTempSummary = document.querySelector("[data-system-temp-summary]");
        const systemNetworkSummary = document.querySelector("[data-system-network-summary]");

        const setTone = (badge, tone) => {
            if (!badge) {
                return;
            }
            badge.classList.remove("is-active", "is-standby", "is-inactive");
            badge.classList.add(`is-${tone}`);
        };

        const updateItem = (item, state, detail, tone) => {
            if (!item) {
                return;
            }
            const stateNode = item.querySelector("[data-overview-state]");
            const detailNode = item.querySelector("[data-overview-detail]");
            const badge = item.querySelector("[data-overview-badge]");
            if (stateNode) {
                stateNode.textContent = state;
            }
            if (detailNode) {
                detailNode.textContent = detail;
            }
            setTone(badge, tone);
        };

        const applyOverviewState = (networkState) => {
            const ethernet = networkState?.ethernet || {};
            const wifiClient = networkState?.wifi_client || {};
            const wifiAp = networkState?.wifi_ap || {};
            const activeUplink = String(networkState?.active_uplink || "none");

            const ethernetConnected = Boolean(ethernet.link_up) && Boolean(ethernet.address);
            const wifiConnected = Boolean(wifiClient.connected_ssid);
            const wifiApEnabled = Boolean(wifiAp.enabled);
            const wifiPresent = wifiClient.present !== false;

            const gatewayHealth = ethernetConnected || wifiConnected || wifiApEnabled ? "Online" : "Standby";
            const primaryLink = activeUplink === "eth0" ? "Ethernet" : activeUplink === "wifi_client" ? "Wi-Fi" : "Offline";
            const wirelessState = wifiConnected ? "Connected" : wifiApEnabled ? "Access Point" : wifiPresent ? "Standby" : "Unavailable";

            if (chipGateway) {
                chipGateway.textContent = gatewayHealth;
            }
            if (chipPrimary) {
                chipPrimary.textContent = primaryLink;
            }
            if (chipWireless) {
                chipWireless.textContent = wirelessState;
            }

            if (led) {
                led.classList.toggle("is-offline", !(ethernetConnected || wifiConnected || wifiApEnabled));
            }
            if (ethLink) {
                ethLink.classList.toggle("is-inactive", !ethernetConnected && activeUplink !== "eth0");
            }
            if (wifiLink) {
                wifiLink.classList.toggle("is-inactive", !wifiConnected && !wifiApEnabled && activeUplink !== "wifi_client");
            }
            if (ethPort) {
                ethPort.classList.toggle("is-active", ethernetConnected || activeUplink === "eth0");
            }
            if (wifiPort) {
                wifiPort.classList.toggle("is-active", wifiConnected || wifiApEnabled || activeUplink === "wifi_client");
            }

            updateItem(
                ethernetItem,
                ethernetConnected ? "Connected" : "Disconnected",
                ethernetConnected ? (ethernet.address || "Address assigned") : "Cable link unavailable",
                ethernetConnected ? "active" : "inactive",
            );

            updateItem(
                wifiItem,
                wifiConnected ? "Connected" : wifiApEnabled ? "Access Point" : wifiPresent ? "Standby" : "Unavailable",
                wifiConnected ? (wifiClient.connected_ssid || "Wireless uplink active") : wifiApEnabled ? `${wifiAp.clients ?? 0} client(s) on hotspot` : wifiPresent ? "Radio available for setup" : "Wireless interface not detected",
                wifiConnected ? "active" : wifiApEnabled || wifiPresent ? "standby" : "inactive",
            );
        };

        const refreshOverviewState = async () => {
            try {
                const [stateResponse, metricsResponse] = await Promise.all([
                    fetch("/api/network/state"),
                    fetch("/api/system/metrics"),
                ]);
                if (!stateResponse.ok) {
                    return;
                }
                const stateData = await stateResponse.json();
                applyOverviewState(stateData);
                if (systemMetricsShell && metricsResponse.ok) {
                    const metrics = await metricsResponse.json();
                    if (systemCpuSummary) {
                        systemCpuSummary.textContent = `${metrics?.cpu?.total_percent ?? 0}% total usage`;
                    }
                    if (systemMemorySummary) {
                        systemMemorySummary.textContent = `${metrics?.memory?.memory_bytes?.used_percent ?? 0}% used`;
                    }
                    if (systemTempSummary) {
                        systemTempSummary.textContent = metrics?.temperature_c != null ? `${metrics.temperature_c} C` : "No reading yet";
                    }
                    if (systemNetworkSummary) {
                        const eth = metrics?.network?.eth0?.rates;
                        const wifi = metrics?.network?.wlan0?.rates;
                        if (eth || wifi) {
                            const parts = [];
                            if (eth) parts.push(`ETH rx ${Math.round(eth.rx_bytes_per_sec)} B/s tx ${Math.round(eth.tx_bytes_per_sec)} B/s`);
                            if (wifi) parts.push(`WIFI rx ${Math.round(wifi.rx_bytes_per_sec)} B/s tx ${Math.round(wifi.tx_bytes_per_sec)} B/s`);
                            systemNetworkSummary.textContent = parts.join(" · ");
                        } else {
                            systemNetworkSummary.textContent = "No samples yet";
                        }
                    }
                }
            } catch (error) {
                console.warn("Failed to refresh overview network state", error);
            }
        };

        refreshOverviewState();
        window.setInterval(refreshOverviewState, 5000);
    }

    const systemShell = document.querySelector("[data-system-shell]");
    if (systemShell) {
        const tabs = Array.from(systemShell.querySelectorAll("[data-system-tab]"));
        const panels = Array.from(systemShell.querySelectorAll("[data-system-panel]"));

        const syncPanels = (tabId) => {
            tabs.forEach((tab) => {
                const isCurrent = tab.getAttribute("data-system-tab") === tabId;
                tab.classList.toggle("is-current", isCurrent);
                tab.setAttribute("aria-selected", isCurrent ? "true" : "false");
            });

            panels.forEach((panel) => {
                const panelId = panel.getAttribute("data-system-panel");
                const visible = panelId === tabId || panelId === `${tabId}-side`;
                panel.classList.toggle("is-hidden", !visible);
            });
        };

        tabs.forEach((tab) => {
            if (tab.hasAttribute("disabled")) {
                return;
            }
            tab.addEventListener("click", () => syncPanels(tab.getAttribute("data-system-tab")));
        });

        const initialTab = tabs.find((tab) => tab.classList.contains("is-current") && !tab.hasAttribute("disabled"))
            || tabs.find((tab) => !tab.hasAttribute("disabled"));
        if (initialTab) {
            syncPanels(initialTab.getAttribute("data-system-tab"));
        }

        const accessForm = systemShell.querySelector("[data-access-form]");
        const accessMessage = systemShell.querySelector("[data-access-message]");
        if (accessForm instanceof HTMLFormElement && accessMessage) {
            accessForm.addEventListener("submit", async (event) => {
                event.preventDefault();
                accessMessage.textContent = "";

                const formData = new FormData(accessForm);
                const payload = {
                    new_username: String(formData.get("new_username") || ""),
                    current_password: String(formData.get("current_password") || ""),
                    new_password: String(formData.get("new_password") || ""),
                    confirm_password: String(formData.get("confirm_password") || ""),
                };

                const button = accessForm.querySelector('button[type="submit"]');
                if (button instanceof HTMLButtonElement) {
                    button.disabled = true;
                    button.textContent = "Saving...";
                }

                try {
                    const response = await fetch("/api/system/access", {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify(payload),
                    });
                    const data = await response.json();
                    accessMessage.textContent = data.message || "Saved.";
                    accessMessage.classList.toggle("is-success", response.ok && data.ok);
                    if (!response.ok || !data.ok) {
                        throw new Error(data.message || "Could not update credentials.");
                    }
                    accessForm.reset();
                    const usernameField = accessForm.querySelector('input[name="current_username"]');
                    const newUsernameField = accessForm.querySelector('input[name="new_username"]');
                    if (usernameField instanceof HTMLInputElement) {
                        usernameField.value = payload.new_username;
                    }
                    if (newUsernameField instanceof HTMLInputElement) {
                        newUsernameField.value = payload.new_username;
                    }
                } catch (error) {
                    accessMessage.textContent = error instanceof Error ? error.message : "Could not update credentials.";
                    accessMessage.classList.remove("is-success");
                } finally {
                    if (button instanceof HTMLButtonElement) {
                        button.disabled = false;
                        button.textContent = "Save credentials";
                    }
                }
            });
        }

    }

    const connectivityShell = document.querySelector("[data-connectivity-shell]");
    if (connectivityShell) {
        const tabs = Array.from(connectivityShell.querySelectorAll("[data-network-tab]"));
        const panels = Array.from(connectivityShell.querySelectorAll("[data-network-panel]"));
        const networkForm = connectivityShell.querySelector("[data-network-form]");
        const networkMessage = connectivityShell.querySelector("[data-network-message]");
        const runtimeToggle = connectivityShell.querySelector("[data-network-runtime-toggle]");
        const runtimePanel = connectivityShell.querySelector("[data-network-runtime-panel]");
        const revertButton = connectivityShell.querySelector("[data-network-revert]");
        const saveButton = connectivityShell.querySelector("[data-network-save]");
        const saveApplyButton = connectivityShell.querySelector("[data-network-save-apply]");
        const scanButton = connectivityShell.querySelector("[data-network-scan]");
        const scanMessage = connectivityShell.querySelector("[data-network-scan-message]");
        const scanResults = connectivityShell.querySelector("[data-network-scan-results]");
        const runtimeEthernetStatus = connectivityShell.querySelector("[data-runtime-ethernet-status]");
        const runtimeEthernetAddress = connectivityShell.querySelector("[data-runtime-ethernet-address]");
        const runtimeWifiStatus = connectivityShell.querySelector("[data-runtime-wifi-status]");
        const runtimeWifiDetail = connectivityShell.querySelector("[data-runtime-wifi-detail]");
        const runtimeApStatus = connectivityShell.querySelector("[data-runtime-ap-status]");
        const runtimeApDetail = connectivityShell.querySelector("[data-runtime-ap-detail]");
        const runtimeApplyStatus = connectivityShell.querySelector("[data-runtime-apply-status]");
        const runtimeApplyTimestamp = connectivityShell.querySelector("[data-runtime-apply-timestamp]");
        const runtimeMonitorStatus = connectivityShell.querySelector("[data-runtime-monitor-status]");
        const runtimeMonitorDetail = connectivityShell.querySelector("[data-runtime-monitor-detail]");
        const wifiSubtabs = Array.from(connectivityShell.querySelectorAll("[data-wifi-subtab]"));
        const wifiSubpanels = Array.from(connectivityShell.querySelectorAll("[data-wifi-subpanel]"));

        const syncWifiSubpanels = (tabId) => {
            wifiSubtabs.forEach((tab) => {
                const isCurrent = tab.getAttribute("data-wifi-subtab") === tabId;
                tab.classList.toggle("is-current", isCurrent);
                tab.setAttribute("aria-selected", isCurrent ? "true" : "false");
            });

            wifiSubpanels.forEach((panel) => {
                const visible = panel.getAttribute("data-wifi-subpanel") === tabId;
                panel.classList.toggle("is-hidden", !visible);
            });
        };

        const syncNetworkPanels = (tabId) => {
            tabs.forEach((tab) => {
                const isCurrent = tab.getAttribute("data-network-tab") === tabId;
                tab.classList.toggle("is-current", isCurrent);
                tab.setAttribute("aria-selected", isCurrent ? "true" : "false");
            });

            panels.forEach((panel) => {
                const visible = panel.getAttribute("data-network-panel") === tabId;
                panel.classList.toggle("is-hidden", !visible);
            });

            if (tabId === "wifi" && wifiSubtabs.length > 0) {
                syncWifiSubpanels("client");
            }
        };

        tabs.forEach((tab) => {
            if (tab.hasAttribute("disabled")) {
                return;
            }
            tab.addEventListener("click", () => syncNetworkPanels(tab.getAttribute("data-network-tab")));
        });

        const initialTab = tabs.find((tab) => tab.classList.contains("is-current") && !tab.hasAttribute("disabled"))
            || tabs.find((tab) => !tab.hasAttribute("disabled"));
        if (initialTab) {
            syncNetworkPanels(initialTab.getAttribute("data-network-tab"));
        }

        wifiSubtabs.forEach((tab) => {
            tab.addEventListener("click", () => syncWifiSubpanels(tab.getAttribute("data-wifi-subtab")));
        });
        if (wifiSubtabs.length > 0) {
            syncWifiSubpanels("client");
        }

        const syncConditionalFields = () => {
            if (!(networkForm instanceof HTMLFormElement)) {
                return;
            }
            const ethernetDhcp = networkForm.elements.namedItem("ethernet_dhcp");
            const wifiClientDhcp = networkForm.elements.namedItem("wifi_client_dhcp");
            const wifiApDhcp = networkForm.elements.namedItem("wifi_ap_dhcp_server_enabled");

            const conditions = {
                "ethernet-static": !(ethernetDhcp instanceof HTMLInputElement && ethernetDhcp.checked),
                "wifi-client-static": !(wifiClientDhcp instanceof HTMLInputElement && wifiClientDhcp.checked),
                "wifi-ap-dhcp": wifiApDhcp instanceof HTMLInputElement && wifiApDhcp.checked,
            };

            const conditionalFields = Array.from(connectivityShell.querySelectorAll("[data-network-visible-when]"));
            conditionalFields.forEach((field) => {
                const key = field.getAttribute("data-network-visible-when");
                field.classList.toggle("is-hidden", !conditions[key]);
            });
        };

        if (networkForm instanceof HTMLFormElement) {
            networkForm.addEventListener("change", syncConditionalFields);
            syncConditionalFields();
        }

        if (runtimeToggle && runtimePanel) {
            runtimeToggle.addEventListener("click", () => {
                const isHidden = runtimePanel.classList.toggle("is-hidden");
                runtimeToggle.textContent = isHidden ? "Show Runtime State" : "Hide Runtime State";
            });
        }

        if (revertButton) {
            revertButton.addEventListener("click", () => window.location.reload());
        }

        const parseDns = (value) =>
            String(value || "")
                .split(",")
                .map((item) => item.trim())
                .filter(Boolean);

        const parseTargets = (value) =>
            String(value || "")
                .split(",")
                .map((item) => item.trim())
                .filter(Boolean);

        const buildNetworkPayload = () => {
            if (!(networkForm instanceof HTMLFormElement)) {
                return null;
            }

            const formData = new FormData(networkForm);
            return {
                version: 1,
                network: {
                    defaults_behavior: {
                        create_defaults_if_missing: true,
                        restore_defaults_if_invalid: true,
                        backup_invalid_file: true,
                    },
                    ethernet: {
                        enabled: formData.get("ethernet_enabled") === "on",
                        interface: String(formData.get("ethernet_interface") || "eth0"),
                        role: String(formData.get("ethernet_role") || "uplink"),
                        dhcp: formData.get("ethernet_dhcp") === "on",
                        static_address: String(formData.get("ethernet_static_address") || "").trim(),
                        static_gateway: String(formData.get("ethernet_static_gateway") || "").trim(),
                        static_dns: parseDns(formData.get("ethernet_static_dns")),
                        route_metric: Number.parseInt(String(formData.get("ethernet_route_metric") || "100"), 10),
                        mtu: Number.parseInt(String(formData.get("ethernet_mtu") || "1500"), 10),
                        uplink_allowed: formData.get("ethernet_uplink_allowed") === "on",
                        downstream_allowed: formData.get("ethernet_downstream_allowed") === "on",
                    },
                    wifi_client: {
                        enabled: formData.get("wifi_client_enabled") === "on",
                        interface: String(formData.get("wifi_client_interface") || "wlan0"),
                        auto_connect: formData.get("wifi_client_auto_connect") === "on",
                        ssid: String(formData.get("wifi_client_ssid") || "").trim(),
                        hidden_ssid: formData.get("wifi_client_hidden_ssid") === "on",
                        security: String(formData.get("wifi_client_security") || "wpa2-psk"),
                        passphrase: String(formData.get("wifi_client_passphrase") || ""),
                        country_code: String(formData.get("wifi_client_country_code") || "").trim().toUpperCase(),
                        band: String(formData.get("wifi_client_band") || "auto"),
                        dhcp: formData.get("wifi_client_dhcp") === "on",
                        static_address: String(formData.get("wifi_client_static_address") || "").trim(),
                        static_gateway: String(formData.get("wifi_client_static_gateway") || "").trim(),
                        static_dns: parseDns(formData.get("wifi_client_static_dns")),
                        route_metric: Number.parseInt(String(formData.get("wifi_client_route_metric") || "200"), 10),
                        uplink_allowed: true,
                    },
                    wifi_ap: {
                        enabled: formData.get("wifi_ap_enabled") === "on",
                        interface: String(formData.get("wifi_ap_interface") || "wlan0"),
                        ssid: String(formData.get("wifi_ap_ssid") || "").trim(),
                        security: String(formData.get("wifi_ap_security") || "wpa2-psk"),
                        passphrase: String(formData.get("wifi_ap_passphrase") || ""),
                        country_code: String(formData.get("wifi_ap_country_code") || "").trim().toUpperCase(),
                        band: String(formData.get("wifi_ap_band") || "2.4ghz"),
                        channel: String(formData.get("wifi_ap_channel") || "auto").trim(),
                        channel_width: "20",
                        subnet_cidr: String(formData.get("wifi_ap_subnet_cidr") || "").trim(),
                        dhcp_server_enabled: formData.get("wifi_ap_dhcp_server_enabled") === "on",
                        dhcp_range_start: String(formData.get("wifi_ap_dhcp_range_start") || "").trim(),
                        dhcp_range_end: String(formData.get("wifi_ap_dhcp_range_end") || "").trim(),
                        nat_enabled: formData.get("wifi_ap_nat_enabled") === "on",
                        client_isolation: formData.get("wifi_ap_client_isolation") === "on",
                        shared_uplink_mode: String(formData.get("wifi_ap_shared_uplink_mode") || "auto"),
                    },
                    cellular: {
                        active_modem_id: "",
                        modems: [],
                    },
                    policy: {
                        uplink_priority: [
                            String(formData.get("policy_uplink_priority_1") || "eth0"),
                            String(formData.get("policy_uplink_priority_2") || "wifi_client"),
                            String(formData.get("policy_uplink_priority_3") || "cellular"),
                        ],
                        failback_enabled: formData.get("policy_failback_enabled") === "on",
                        stable_seconds_before_switch: Number.parseInt(String(formData.get("policy_stable_seconds_before_switch") || "5"), 10),
                        require_connectivity_check: formData.get("policy_require_connectivity_check") === "on",
                        fail_count_threshold: Number.parseInt(String(formData.get("policy_fail_count_threshold") || "1"), 10),
                        recover_count_threshold: Number.parseInt(String(formData.get("policy_recover_count_threshold") || "1"), 10),
                        connectivity_targets: parseTargets(formData.get("policy_connectivity_targets") || "1.1.1.1, 8.8.8.8"),
                    },
                },
            };
        };

        const validateNetworkPayload = (payload) => {
            const wifiClientEnabled = Boolean(payload?.network?.wifi_client?.enabled);
            const wifiApEnabled = Boolean(payload?.network?.wifi_ap?.enabled);
            const sharedUplinkMode = String(payload?.network?.wifi_ap?.shared_uplink_mode || "auto");

            if (wifiClientEnabled && wifiApEnabled) {
                return "Current gateway image supports either Wi-Fi client or Wi-Fi AP on wlan0, not both at the same time.";
            }

            if (!["auto", "ethernet", "eth0"].includes(sharedUplinkMode)) {
                return "Current gateway image supports Wi-Fi AP sharing only through Ethernet or Auto.";
            }

            return "";
        };

        const updateRuntimeState = (networkState, applyResult) => {
            if (runtimeEthernetStatus) {
                runtimeEthernetStatus.textContent = networkState?.ethernet?.link_up ? "Link up" : "Link down";
            }
            if (runtimeEthernetAddress) {
                const ethAddress = networkState?.ethernet?.address || "No address assigned";
                const ethInternet = networkState?.ethernet?.internet_ok ? "Internet OK" : "Internet pending";
                runtimeEthernetAddress.textContent = `${ethAddress} · ${ethInternet}`;
            }

            if (runtimeWifiStatus) {
                runtimeWifiStatus.textContent = networkState?.wifi_client?.connected_ssid ? "Connected" : "Disconnected";
            }
            if (runtimeWifiDetail) {
                const wifiBase = networkState?.wifi_client?.connected_ssid || networkState?.wifi_client?.address || "No active SSID";
                const wifiInternet = networkState?.wifi_client?.internet_ok ? "Internet OK" : "Internet pending";
                const wifiText = networkState?.wifi_client?.connected_ssid || networkState?.wifi_client?.address
                    ? `${wifiBase} · ${wifiInternet}`
                    : wifiBase;
                runtimeWifiDetail.textContent = wifiText;
            }

            if (runtimeApStatus) {
                runtimeApStatus.textContent = networkState?.wifi_ap?.enabled ? "Enabled" : "Disabled";
            }
            if (runtimeApDetail) {
                runtimeApDetail.textContent = `${networkState?.wifi_ap?.clients ?? 0} client(s)`;
            }

            if (runtimeApplyStatus) {
                const statusText = String(applyResult?.status || "unknown").replaceAll("_", " ");
                runtimeApplyStatus.textContent = statusText.charAt(0).toUpperCase() + statusText.slice(1);
            }
            if (runtimeApplyTimestamp) {
                runtimeApplyTimestamp.textContent = applyResult?.timestamp || "No apply run yet";
            }

            if (runtimeMonitorStatus) {
                const statusText = String(networkState?.monitor_status || "unknown").replaceAll("_", " ");
                runtimeMonitorStatus.textContent = statusText.charAt(0).toUpperCase() + statusText.slice(1);
            }
            if (runtimeMonitorDetail) {
                const recovery = networkState?.recovery || {};
                runtimeMonitorDetail.textContent = recovery?.last_reason
                    ? `Recovery ${recovery.count ?? 0} · ${recovery.last_reason}`
                    : "No recovery action recorded";
            }
        };

        const refreshRuntimeState = async () => {
            try {
                const [stateResponse, applyResponse] = await Promise.all([
                    fetch("/api/network/state"),
                    fetch("/api/network/apply-result"),
                ]);
                if (!stateResponse.ok || !applyResponse.ok) {
                    return;
                }
                const [stateData, applyData] = await Promise.all([stateResponse.json(), applyResponse.json()]);
                updateRuntimeState(stateData, applyData);
            } catch (error) {
                console.warn("Failed to refresh runtime network state", error);
            }
        };

        const renderScanResults = (networks) => {
            if (!scanResults) {
                return;
            }

            if (!Array.isArray(networks) || networks.length === 0) {
                scanResults.innerHTML = '<p class="settings-inline-note">No Wi-Fi networks were found in the last scan.</p>';
                return;
            }

            scanResults.innerHTML = networks
                .map((network, index) => `
                    <button type="button" class="ghost-action" data-network-scan-select="${index}">
                        ${network.ssid || "(Hidden SSID)"} · ${network.band} · ${network.signal_dbm} dBm · ${network.security}
                    </button>
                `)
                .join("");

            const ssidInput = networkForm?.elements?.namedItem("wifi_client_ssid");
            const bandInput = networkForm?.elements?.namedItem("wifi_client_band");
            const securityInput = networkForm?.elements?.namedItem("wifi_client_security");

            scanResults.querySelectorAll("[data-network-scan-select]").forEach((button) => {
                button.addEventListener("click", () => {
                    const index = Number.parseInt(button.getAttribute("data-network-scan-select") || "-1", 10);
                    const network = networks[index];
                    if (!network) {
                        return;
                    }
                    if (ssidInput instanceof HTMLInputElement) {
                        ssidInput.value = network.ssid || "";
                    }
                    if (bandInput instanceof HTMLSelectElement && ["auto", "2.4ghz", "5ghz"].includes(network.band)) {
                        bandInput.value = network.band;
                    }
                    if (securityInput instanceof HTMLSelectElement) {
                        securityInput.value = network.security === "open" ? "open" : "wpa2-psk";
                    }
                });
            });
        };

        const runNetworkAction = async (endpoint, activeButton, busyLabel, idleLabel) => {
            if (!(networkForm instanceof HTMLFormElement) || !networkMessage) {
                return;
            }

            const payload = buildNetworkPayload();
            if (!payload) {
                return;
            }

            const validationMessage = validateNetworkPayload(payload);
            if (validationMessage) {
                networkMessage.textContent = validationMessage;
                networkMessage.classList.remove("is-success");
                return;
            }

            networkMessage.textContent = "";
            networkMessage.classList.remove("is-success");

            const buttons = [saveButton, saveApplyButton].filter((button) => button instanceof HTMLButtonElement);
            buttons.forEach((button) => {
                button.disabled = true;
            });
            if (activeButton instanceof HTMLButtonElement) {
                activeButton.textContent = busyLabel;
            }

            try {
                const response = await fetch(endpoint, {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify(payload),
                });
                const data = await response.json();
                const firstError = Array.isArray(data.errors) && data.errors.length > 0 ? data.errors[0].message : null;
                const message = firstError || data.message || data.apply_status || "Saved.";
                networkMessage.textContent = message;
                networkMessage.classList.toggle("is-success", response.ok && data.ok);
                await refreshRuntimeState();
                if (!response.ok || !data.ok) {
                    throw new Error(message);
                }
            } catch (error) {
                networkMessage.textContent = error instanceof Error ? error.message : "Could not save network settings.";
                networkMessage.classList.remove("is-success");
            } finally {
                buttons.forEach((button) => {
                    button.disabled = false;
                });
                if (saveButton instanceof HTMLButtonElement) {
                    saveButton.textContent = "Save";
                }
                if (saveApplyButton instanceof HTMLButtonElement) {
                    saveApplyButton.textContent = "Save and Apply";
                }
                if (activeButton instanceof HTMLButtonElement) {
                    activeButton.textContent = idleLabel;
                }
            }
        };

        if (saveButton) {
            saveButton.addEventListener("click", () => {
                runNetworkAction("/api/network/settings", saveButton, "Saving...", "Save");
            });
        }

        if (saveApplyButton) {
            saveApplyButton.addEventListener("click", () => {
                runNetworkAction("/api/network/save-and-apply", saveApplyButton, "Applying...", "Save and Apply");
            });
        }

        if (scanButton) {
            scanButton.addEventListener("click", async () => {
                if (scanButton instanceof HTMLButtonElement) {
                    scanButton.disabled = true;
                    scanButton.textContent = "Scanning...";
                }
                if (scanMessage) {
                    scanMessage.classList.add("is-hidden");
                    scanMessage.textContent = "";
                }

                try {
                    const response = await fetch("/api/network/wifi/scan", { method: "POST" });
                    const data = await response.json();
                    if (!response.ok || !data.ok) {
                        throw new Error((data.errors && data.errors[0]?.message) || "Wi-Fi scan failed.");
                    }
                    renderScanResults(data.networks || []);
                } catch (error) {
                    if (scanMessage) {
                        scanMessage.classList.remove("is-hidden");
                        scanMessage.textContent = error instanceof Error ? error.message : "Wi-Fi scan failed.";
                    }
                } finally {
                    if (scanButton instanceof HTMLButtonElement) {
                        scanButton.disabled = false;
                        scanButton.textContent = "Scan Wi-Fi";
                    }
                }
            });
        }

        refreshRuntimeState();
        window.setInterval(refreshRuntimeState, 5000);
    }

    const loginForm = document.querySelector(".auth-form");
    const loginError = document.querySelector("[data-login-error]");

    if (!loginForm) {
        return;
    }

    loginForm.addEventListener("submit", async (event) => {
        event.preventDefault();

        const formData = new FormData(loginForm);
        const payload = {
            username: String(formData.get("username") || ""),
            password: String(formData.get("password") || ""),
        };

        if (loginError) {
            loginError.textContent = "";
        }

        const submitButton = loginForm.querySelector('button[type="submit"]');
        if (submitButton instanceof HTMLButtonElement) {
            submitButton.disabled = true;
            submitButton.textContent = "Signing in...";
        }

        try {
            const response = await fetch("/api/login", {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                },
                body: JSON.stringify(payload),
            });

            const data = await response.json();

            if (!response.ok || !data.ok) {
                throw new Error(data.message || "Login failed.");
            }

            window.location.href = data.redirect || "/dashboard";
        } catch (error) {
            if (loginError) {
                loginError.textContent = error instanceof Error ? error.message : "Login failed.";
            }
        } finally {
            if (submitButton instanceof HTMLButtonElement) {
                submitButton.disabled = false;
                submitButton.textContent = "Enter Control Plane";
            }
        }
    });
});
