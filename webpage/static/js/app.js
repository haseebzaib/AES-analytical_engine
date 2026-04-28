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
            const eth0 = networkState?.eth0 || {};
            const eth1 = networkState?.eth1 || {};
            const wifiClient = networkState?.wifi_client || {};
            const wifiAp = networkState?.wifi_ap || {};
            const activeUplink = String(networkState?.active_uplink || "none");

            const eth0Connected = Boolean(eth0.link_up) && Boolean(eth0.address);
            const eth1Connected = Boolean(eth1.link_up) && Boolean(eth1.address);
            const ethernetConnected = eth0Connected || eth1Connected;
            const wifiConnected = Boolean(wifiClient.connected_ssid);
            const wifiApEnabled = Boolean(wifiAp.enabled);
            const wifiPresent = wifiClient.present !== false;

            const gatewayHealth = ethernetConnected || wifiConnected || wifiApEnabled ? "Online" : "Standby";
            const primaryLink = ["eth0","eth1"].includes(activeUplink) ? "Ethernet" : activeUplink === "wifi_client" ? "Wi-Fi" : "Offline";
            const wirelessState = wifiConnected ? "Connected" : wifiApEnabled ? "Access Point" : wifiPresent ? "Standby" : "Unavailable";

            if (chipGateway) chipGateway.textContent = gatewayHealth;
            if (chipPrimary) chipPrimary.textContent = primaryLink;
            if (chipWireless) chipWireless.textContent = wirelessState;

            if (led) {
                led.classList.toggle("is-offline", !(ethernetConnected || wifiConnected || wifiApEnabled));
            }
            if (ethLink) {
                ethLink.classList.toggle("is-inactive", !ethernetConnected && !["eth0","eth1"].includes(activeUplink));
            }
            if (wifiLink) {
                wifiLink.classList.toggle("is-inactive", !wifiConnected && !wifiApEnabled && activeUplink !== "wifi_client");
            }
            if (ethPort) {
                ethPort.classList.toggle("is-active", ethernetConnected || ["eth0","eth1"].includes(activeUplink));
            }
            if (wifiPort) {
                wifiPort.classList.toggle("is-active", wifiConnected || wifiApEnabled || activeUplink === "wifi_client");
            }

            // Show active uplink address, fall back to whichever eth has an address
            const ethAddress = eth0.address || eth1.address || "";
            const ethDetail = ethAddress
                ? `${activeUplink === "eth1" ? "eth1" : "eth0"}: ${ethAddress}`
                : "Waiting for DHCP";
            updateItem(
                ethernetItem,
                ethernetConnected ? "Connected" : "Disconnected",
                ethernetConnected ? ethDetail : "No cable link",
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
                        const eth0 = metrics?.network?.eth0?.rates;
                        const eth1 = metrics?.network?.eth1?.rates;
                        const wifi = metrics?.network?.wlan0?.rates;
                        if (eth0 || eth1 || wifi) {
                            const parts = [];
                            if (eth0) parts.push(`ETH0 rx ${Math.round(eth0.rx_bytes_per_sec)} B/s tx ${Math.round(eth0.tx_bytes_per_sec)} B/s`);
                            if (eth1) parts.push(`ETH1 rx ${Math.round(eth1.rx_bytes_per_sec)} B/s tx ${Math.round(eth1.tx_bytes_per_sec)} B/s`);
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
                version: 2,
                network: {
                    defaults_behavior: {
                        create_defaults_if_missing: true,
                        restore_defaults_if_invalid: true,
                        backup_invalid_file: true,
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
                    uplink: {
                        uplink_priority: [
                            String(formData.get("uplink_priority_1") || "eth0"),
                            String(formData.get("uplink_priority_2") || "eth1"),
                            String(formData.get("uplink_priority_3") || "wifi_client"),
                            String(formData.get("uplink_priority_4") || "cellular"),
                        ],
                        failback_enabled: formData.get("uplink_failback_enabled") === "on",
                        stable_seconds_before_switch: Number.parseInt(String(formData.get("uplink_stable_seconds_before_switch") || "5"), 10),
                        require_connectivity_check: formData.get("uplink_require_connectivity_check") === "on",
                        fail_count_threshold: Number.parseInt(String(formData.get("uplink_fail_count_threshold") || "1"), 10),
                        recover_count_threshold: Number.parseInt(String(formData.get("uplink_recover_count_threshold") || "1"), 10),
                        connectivity_targets: parseTargets(formData.get("uplink_connectivity_targets") || "1.1.1.1, 8.8.8.8"),
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
            const runtimeEth0Status = connectivityShell.querySelector("[data-runtime-eth0-status]");
            const runtimeEth0Address = connectivityShell.querySelector("[data-runtime-eth0-address]");
            const runtimeEth1Status = connectivityShell.querySelector("[data-runtime-eth1-status]");
            const runtimeEth1Address = connectivityShell.querySelector("[data-runtime-eth1-address]");
            if (runtimeEth0Status) runtimeEth0Status.textContent = networkState?.eth0?.link_up ? "Link up" : "Link down";
            if (runtimeEth0Address) runtimeEth0Address.textContent = networkState?.eth0?.address || "No address";
            if (runtimeEth1Status) runtimeEth1Status.textContent = networkState?.eth1?.link_up ? "Link up" : "Link down";
            if (runtimeEth1Address) runtimeEth1Address.textContent = networkState?.eth1?.address || "No address";

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
                if (!stateResponse.ok || !applyResponse.ok) return;
                const [stateData, applyData] = await Promise.all([stateResponse.json(), applyResponse.json()]);
                updateRuntimeState(stateData, applyData);
            } catch (error) {
                console.warn("Failed to refresh runtime network state", error);
            }
        };

        // Auto-refresh runtime panel every 5s while it's visible
        let runtimeRefreshTimer = null;
        if (runtimeToggle && runtimePanel) {
            runtimeToggle.addEventListener("click", () => {
                const isVisible = !runtimePanel.classList.contains("is-hidden");
                if (!isVisible) {
                    refreshRuntimeState();
                    runtimeRefreshTimer = setInterval(refreshRuntimeState, 5000);
                } else {
                    clearInterval(runtimeRefreshTimer);
                }
            });
        }

        // Poll connection status after Save and Apply
        const startConnectionPoll = (wifiEnabled) => {
            const strip = connectivityShell.querySelector("[data-net-apply-strip]");
            const dot   = connectivityShell.querySelector("[data-net-apply-dot]");
            const title = connectivityShell.querySelector("[data-net-apply-title]");
            const detail = connectivityShell.querySelector("[data-net-apply-detail]");
            const uplinkBadge = connectivityShell.querySelector("[data-net-apply-uplink]");
            if (!strip) return;

            strip.style.display = "";
            strip.className = "net-apply-strip is-pending";
            title.textContent = wifiEnabled ? "Connecting to Wi-Fi…" : "Applying network configuration…";
            detail.textContent = "Waiting for network monitor…";
            if (uplinkBadge) uplinkBadge.textContent = "";

            const MAX_POLLS = 20;
            const INTERVAL_MS = 2000;
            let polls = 0;
            let pollTimer = null;

            const stopPoll = () => clearTimeout(pollTimer);

            const tick = async () => {
                polls++;
                try {
                    const res = await fetch("/api/network/state");
                    if (!res.ok) throw new Error("state fetch failed");
                    const state = await res.json();
                    updateRuntimeState(state, {});

                    const activeUplink = String(state.active_uplink || "none");
                    const wifiSsid    = state.wifi_client?.connected_ssid;
                    const wifiAddr    = state.wifi_client?.address;
                    const eth0Addr    = state.eth0?.address;
                    const eth1Addr    = state.eth1?.address;

                    if (wifiEnabled && wifiSsid) {
                        strip.className = "net-apply-strip is-success";
                        title.textContent = `Connected — ${wifiSsid}`;
                        detail.textContent = wifiAddr || "Address assigned";
                        if (uplinkBadge) uplinkBadge.textContent = "Active uplink";
                        return stopPoll();
                    }

                    if (!wifiEnabled && ["eth0", "eth1"].includes(activeUplink)) {
                        const addr = activeUplink === "eth1" ? eth1Addr : eth0Addr;
                        strip.className = "net-apply-strip is-success";
                        title.textContent = "Configuration applied";
                        detail.textContent = addr ? `${activeUplink}: ${addr}` : activeUplink;
                        if (uplinkBadge) uplinkBadge.textContent = "Active uplink";
                        return stopPoll();
                    }

                    if (polls >= MAX_POLLS) {
                        strip.className = "net-apply-strip is-error";
                        title.textContent = wifiEnabled ? "Wi-Fi did not connect" : "No uplink established";
                        detail.textContent = wifiEnabled
                            ? "Check SSID, password, and signal strength."
                            : "Check cable or network configuration.";
                        return stopPoll();
                    }

                    detail.textContent = `Checking… (${polls * 2}s elapsed)`;
                } catch {
                    detail.textContent = "Could not reach gateway.";
                }
                pollTimer = setTimeout(tick, INTERVAL_MS);
            };

            pollTimer = setTimeout(tick, INTERVAL_MS);
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

        const runNetworkAction = async (endpoint, activeButton, busyLabel, idleLabel, afterSuccess = null) => {
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
                if (afterSuccess) afterSuccess();
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
                const wifiEnabled = networkForm?.elements?.namedItem("wifi_client_enabled") instanceof HTMLInputElement
                    && networkForm.elements.namedItem("wifi_client_enabled").checked;
                runNetworkAction(
                    "/api/network/save-and-apply",
                    saveApplyButton,
                    "Applying…",
                    "Save and Apply",
                    () => startConnectionPoll(wifiEnabled),
                );
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

    const monitorShell = document.querySelector("[data-monitor-shell]");
    if (monitorShell) {
        const drawSparkline = (container, data, opts = {}) => {
            if (!container || !Array.isArray(data) || data.length < 2) return;
            const { stroke = "#39d0c8", min: minOverride, max: maxOverride, fmt = (v) => v.toFixed(1) } = opts;
            const W = container.clientWidth || 300;
            const H = container.clientHeight || 72;
            const pad = 3;
            const uH = H - pad * 2;
            const uW = W - pad * 2;
            const minV = minOverride !== undefined ? minOverride : Math.min(...data);
            const maxV = maxOverride !== undefined ? maxOverride : Math.max(...data);
            const range = maxV - minV || 1;
            const toX = (i) => pad + (i / (data.length - 1)) * uW;
            const toY = (v) => pad + uH - ((v - minV) / range) * uH;
            const pts = data.map((v, i) => `${toX(i)},${toY(v)}`).join(" ");
            const gradId = `sg${stroke.replace(/[^a-z0-9]/gi, "")}`;
            container.innerHTML = `<svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="none" xmlns="http://www.w3.org/2000/svg">
                <defs>
                    <linearGradient id="${gradId}" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="0%" stop-color="${stroke}" stop-opacity="0.32"/>
                        <stop offset="100%" stop-color="${stroke}" stop-opacity="0.03"/>
                    </linearGradient>
                </defs>
                <path d="M ${toX(0)},${toY(data[0])} L ${pts} L ${toX(data.length - 1)},${H} L ${toX(0)},${H} Z" fill="url(#${gradId})"/>
                <polyline points="${pts}" fill="none" stroke="${stroke}" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>
                <line class="ch-xhair" x1="0" y1="${pad}" x2="0" y2="${H - pad}" stroke="${stroke}" stroke-width="1" stroke-opacity="0.45" stroke-dasharray="3 2" opacity="0"/>
                <circle class="ch-dot" r="3.5" fill="${stroke}" stroke="#08141a" stroke-width="1.5" opacity="0"/>
                <g class="ch-tip" opacity="0">
                    <rect class="ch-tip-bg" rx="3" fill="rgba(8,20,26,0.92)" stroke="${stroke}" stroke-width="0.75" stroke-opacity="0.6"/>
                    <text class="ch-tip-txt" font-size="10" font-family="monospace" fill="${stroke}" text-anchor="middle" dominant-baseline="middle"/>
                </g>
                <rect class="ch-overlay" x="${pad}" y="${pad}" width="${uW}" height="${uH}" fill="transparent" style="cursor:crosshair"/>
            </svg>`;

            const svg = container.querySelector("svg");
            const xhair = svg.querySelector(".ch-xhair");
            const dot = svg.querySelector(".ch-dot");
            const tip = svg.querySelector(".ch-tip");
            const tipBg = svg.querySelector(".ch-tip-bg");
            const tipTxt = svg.querySelector(".ch-tip-txt");
            const overlay = svg.querySelector(".ch-overlay");
            const tipPadX = 6;
            const tipH = 16;

            overlay.addEventListener("mousemove", (e) => {
                const rect = svg.getBoundingClientRect();
                const mx = (e.clientX - rect.left) * (W / rect.width);
                const idx = Math.round(Math.max(0, Math.min(1, (mx - pad) / uW)) * (data.length - 1));
                const x = toX(idx);
                const y = toY(data[idx]);
                const label = fmt(data[idx]);
                const textW = label.length * 6 + tipPadX * 2;

                xhair.setAttribute("x1", x); xhair.setAttribute("x2", x); xhair.setAttribute("opacity", "1");
                dot.setAttribute("cx", x); dot.setAttribute("cy", y); dot.setAttribute("opacity", "1");

                let tx = x - textW / 2;
                let ty = y - tipH - 6;
                if (tx < pad) tx = pad;
                if (tx + textW > W - pad) tx = W - pad - textW;
                if (ty < pad) ty = y + 8;
                if (ty + tipH > H - pad) ty = H - pad - tipH;

                tipBg.setAttribute("x", tx); tipBg.setAttribute("y", ty);
                tipBg.setAttribute("width", textW); tipBg.setAttribute("height", tipH);
                tipTxt.setAttribute("x", tx + textW / 2); tipTxt.setAttribute("y", ty + tipH / 2);
                tipTxt.textContent = label;
                tip.setAttribute("opacity", "1");
            });

            overlay.addEventListener("mouseleave", () => {
                xhair.setAttribute("opacity", "0");
                dot.setAttribute("opacity", "0");
                tip.setAttribute("opacity", "0");
            });
        };

        const drawDualSparkline = (container, rxData, txData) => {
            if (!container || !Array.isArray(rxData) || rxData.length < 2) return;
            const W = container.clientWidth || 300;
            const H = container.clientHeight || 72;
            const pad = 3;
            const uH = H - pad * 2;
            const uW = W - pad * 2;
            const allVals = [...rxData, ...(Array.isArray(txData) ? txData : [])];
            const maxV = Math.max(1, ...allVals);
            const toX = (i) => pad + (i / (rxData.length - 1)) * uW;
            const toY = (v) => pad + uH - (v / maxV) * uH;
            const rxPts = rxData.map((v, i) => `${toX(i)},${toY(v)}`).join(" ");
            const hasTx = Array.isArray(txData) && txData.length >= 2;
            const txPts = hasTx ? txData.map((v, i) => `${toX(i)},${toY(v)}`).join(" ") : null;
            container.innerHTML = `<svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="none" xmlns="http://www.w3.org/2000/svg">
                <defs>
                    <linearGradient id="sg_netrx" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="0%" stop-color="#a97df0" stop-opacity="0.28"/>
                        <stop offset="100%" stop-color="#a97df0" stop-opacity="0.02"/>
                    </linearGradient>
                </defs>
                <path d="M ${toX(0)},${toY(rxData[0])} L ${rxPts} L ${toX(rxData.length - 1)},${H} L ${toX(0)},${H} Z" fill="url(#sg_netrx)"/>
                <polyline points="${rxPts}" fill="none" stroke="#a97df0" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>
                ${txPts ? `<polyline points="${txPts}" fill="none" stroke="#f0a64b" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round" stroke-dasharray="4 3"/>` : ""}
                <line class="ch-xhair" x1="0" y1="${pad}" x2="0" y2="${H - pad}" stroke="rgba(255,255,255,0.35)" stroke-width="1" stroke-dasharray="3 2" opacity="0"/>
                <circle class="ch-dot-rx" r="3" fill="#a97df0" stroke="#08141a" stroke-width="1.5" opacity="0"/>
                ${hasTx ? `<circle class="ch-dot-tx" r="3" fill="#f0a64b" stroke="#08141a" stroke-width="1.5" opacity="0"/>` : ""}
                <g class="ch-tip" opacity="0">
                    <rect class="ch-tip-bg" rx="3" fill="rgba(8,20,26,0.92)" stroke="rgba(255,255,255,0.18)" stroke-width="0.75"/>
                    <text class="ch-tip-rx" font-size="9" font-family="monospace" fill="#a97df0" dominant-baseline="middle"/>
                    ${hasTx ? `<text class="ch-tip-tx" font-size="9" font-family="monospace" fill="#f0a64b" dominant-baseline="middle"/>` : ""}
                </g>
                <rect class="ch-overlay" x="${pad}" y="${pad}" width="${uW}" height="${uH}" fill="transparent" style="cursor:crosshair"/>
            </svg>`;

            const svg = container.querySelector("svg");
            const xhair = svg.querySelector(".ch-xhair");
            const dotRx = svg.querySelector(".ch-dot-rx");
            const dotTx = svg.querySelector(".ch-dot-tx");
            const tip = svg.querySelector(".ch-tip");
            const tipBg = svg.querySelector(".ch-tip-bg");
            const tipRxEl = svg.querySelector(".ch-tip-rx");
            const tipTxEl = svg.querySelector(".ch-tip-tx");
            const overlay = svg.querySelector(".ch-overlay");
            const tipPadX = 6;
            const tipPadY = 4;
            const lineH = 11;

            overlay.addEventListener("mousemove", (e) => {
                const rect = svg.getBoundingClientRect();
                const mx = (e.clientX - rect.left) * (W / rect.width);
                const idx = Math.round(Math.max(0, Math.min(1, (mx - pad) / uW)) * (rxData.length - 1));
                const x = toX(idx);
                const yRx = toY(rxData[idx]);
                const rxLabel = `rx ${fmtBps(rxData[idx])}`;
                const txLabel = hasTx ? `tx ${fmtBps(txData[idx])}` : null;
                const longestLabel = txLabel && txLabel.length > rxLabel.length ? txLabel : rxLabel;
                const textW = longestLabel.length * 5.5 + tipPadX * 2;
                const tipH = hasTx ? tipPadY * 2 + lineH * 2 : tipPadY * 2 + lineH;

                xhair.setAttribute("x1", x); xhair.setAttribute("x2", x); xhair.setAttribute("opacity", "1");
                dotRx.setAttribute("cx", x); dotRx.setAttribute("cy", yRx); dotRx.setAttribute("opacity", "1");
                if (dotTx) {
                    dotTx.setAttribute("cx", x); dotTx.setAttribute("cy", toY(txData[idx])); dotTx.setAttribute("opacity", "1");
                }

                let tx = x - textW / 2;
                let ty = yRx - tipH - 6;
                if (tx < pad) tx = pad;
                if (tx + textW > W - pad) tx = W - pad - textW;
                if (ty < pad) ty = yRx + 8;
                if (ty + tipH > H - pad) ty = H - pad - tipH;

                tipBg.setAttribute("x", tx); tipBg.setAttribute("y", ty);
                tipBg.setAttribute("width", textW); tipBg.setAttribute("height", tipH);
                tipRxEl.setAttribute("x", tx + tipPadX); tipRxEl.setAttribute("y", ty + tipPadY + lineH / 2);
                tipRxEl.textContent = rxLabel;
                if (tipTxEl && txLabel) {
                    tipTxEl.setAttribute("x", tx + tipPadX); tipTxEl.setAttribute("y", ty + tipPadY + lineH + lineH / 2);
                    tipTxEl.textContent = txLabel;
                }
                tip.setAttribute("opacity", "1");
            });

            overlay.addEventListener("mouseleave", () => {
                xhair.setAttribute("opacity", "0");
                dotRx.setAttribute("opacity", "0");
                if (dotTx) dotTx.setAttribute("opacity", "0");
                tip.setAttribute("opacity", "0");
            });
        };

        const fmtBps = (bps) => {
            if (bps >= 1048576) return `${(bps / 1048576).toFixed(1)} MB/s`;
            if (bps >= 1024) return `${(bps / 1024).toFixed(1)} KB/s`;
            return `${Math.round(bps)} B/s`;
        };

        const fmtBytes = (b) => {
            if (b >= 1073741824) return `${(b / 1073741824).toFixed(1)} GB`;
            if (b >= 1048576) return `${(b / 1048576).toFixed(0)} MB`;
            return `${Math.round(b / 1024)} KB`;
        };

        const setKpiBar = (name, pct) => {
            const bar = monitorShell.querySelector(`[data-kpi-bar="${name}"]`);
            if (!bar) return;
            bar.style.width = `${Math.min(100, Math.max(0, pct))}%`;
            if (name === "temp") {
                bar.classList.toggle("is-warm", pct >= 50 && pct < 70);
                bar.classList.toggle("is-hot", pct >= 70);
            }
        };

        const applyCurrentMetrics = (m) => {
            if (!m?.cpu) return;

            // CPU — m.cpu.total_percent, m.cpu.per_core: [{core, usage_percent}]
            const cpuPct = m.cpu.total_percent ?? 0;
            const perCore = Array.isArray(m.cpu.per_core) ? m.cpu.per_core : [];
            const cpuVal = monitorShell.querySelector("[data-kpi-value=\"cpu\"]");
            const cpuSub = monitorShell.querySelector("[data-kpi-sub=\"cpu\"]");
            const cpuLive = monitorShell.querySelector("[data-chart-live=\"cpu\"]");
            if (cpuVal) cpuVal.textContent = `${cpuPct}%`;
            if (cpuSub) cpuSub.textContent = `${perCore.length} core${perCore.length !== 1 ? "s" : ""}`;
            if (cpuLive) cpuLive.textContent = `${cpuPct}%`;
            setKpiBar("cpu", cpuPct);

            // Memory — m.memory.memory_bytes.{used_percent, used, total}
            const memPct = m.memory?.memory_bytes?.used_percent ?? 0;
            const memUsed = m.memory?.memory_bytes?.used ?? 0;
            const memTotal = m.memory?.memory_bytes?.total ?? 0;
            const memVal = monitorShell.querySelector("[data-kpi-value=\"memory\"]");
            const memSub = monitorShell.querySelector("[data-kpi-sub=\"memory\"]");
            const memLive = monitorShell.querySelector("[data-chart-live=\"memory\"]");
            if (memVal) memVal.textContent = `${memPct}%`;
            if (memSub) memSub.textContent = memTotal ? `${fmtBytes(memUsed)} of ${fmtBytes(memTotal)}` : "No data";
            if (memLive) memLive.textContent = `${memPct}%`;
            setKpiBar("memory", memPct);

            // Temperature
            const tempC = m.temperature_c ?? null;
            const tempVal = monitorShell.querySelector("[data-kpi-value=\"temp\"]");
            const tempSub = monitorShell.querySelector("[data-kpi-sub=\"temp\"]");
            const tempLive = monitorShell.querySelector("[data-chart-live=\"temp\"]");
            if (tempVal) tempVal.textContent = tempC != null ? `${tempC}\u00b0C` : "--";
            if (tempSub) tempSub.textContent = tempC == null ? "No sensor" : tempC < 50 ? "Normal" : tempC < 70 ? "Warm" : "Hot";
            if (tempLive) tempLive.textContent = tempC != null ? `${tempC} \u00b0C` : "-- \u00b0C";
            setKpiBar("temp", tempC != null ? Math.min(100, (tempC / 85) * 100) : 0);

            // Filesystem — m.filesystem.{used_percent, used_bytes, total_bytes}
            const diskPct = m.filesystem?.used_percent ?? 0;
            const diskUsed = m.filesystem?.used_bytes ?? 0;
            const diskTotal = m.filesystem?.total_bytes ?? 0;
            const diskVal = monitorShell.querySelector("[data-kpi-value=\"disk\"]");
            const diskSub = monitorShell.querySelector("[data-kpi-sub=\"disk\"]");
            if (diskVal) diskVal.textContent = `${diskPct}%`;
            if (diskSub) diskSub.textContent = diskTotal ? `${fmtBytes(diskUsed)} of ${fmtBytes(diskTotal)}` : "No data";
            setKpiBar("disk", diskPct);

            // Per-core bars — each entry is {core: int, usage_percent: float}
            const coreGrid = monitorShell.querySelector("[data-core-grid]");
            if (coreGrid && perCore.length > 0) {
                coreGrid.innerHTML = perCore.map((c) => `<div class="core-item">
                    <div class="core-bar-track"><div class="core-bar-fill" style="height:${Math.min(100, c.usage_percent)}%"></div></div>
                    <p class="core-item-value">${c.usage_percent}%</p>
                    <p class="core-item-label">C${c.core}</p>
                </div>`).join("");
            }

            // Load average — m.cpu.load_average: {"1m", "5m", "15m"}
            const loadAvg = m.cpu.load_average ?? {};
            ["1m", "5m", "15m"].forEach((key) => {
                const el = monitorShell.querySelector(`[data-load-avg="${key}"]`);
                if (el) el.textContent = loadAvg[key] != null ? Number(loadAvg[key]).toFixed(2) : "--";
            });

            // Network rates
            const eth0Rates = m.network?.eth0?.rates;
            const eth1Rates = m.network?.eth1?.rates;
            const wifiRates = m.network?.wlan0?.rates;
            const netLive = monitorShell.querySelector("[data-chart-live=\"network\"]");
            if (netLive) {
                const rx = (eth0Rates?.rx_bytes_per_sec ?? 0) + (eth1Rates?.rx_bytes_per_sec ?? 0) + (wifiRates?.rx_bytes_per_sec ?? 0);
                netLive.textContent = fmtBps(rx);
            }
            ["eth0", "eth1", "wlan0"].forEach((iface) => {
                const rates = m.network?.[iface]?.rates;
                const rxEl = monitorShell.querySelector(`[data-net-rx="${iface}"]`);
                const txEl = monitorShell.querySelector(`[data-net-tx="${iface}"]`);
                if (rxEl) rxEl.textContent = `rx ${rates ? fmtBps(rates.rx_bytes_per_sec) : "--"}`;
                if (txEl) txEl.textContent = `tx ${rates ? fmtBps(rates.tx_bytes_per_sec) : "--"}`;
            });
        };

        const applyHistoryMetrics = (history) => {
            const samples = Array.isArray(history?.samples) ? history.samples : [];
            if (samples.length < 2) return;
            const cpuData = samples.map((s) => s.cpu_total_percent ?? 0);
            const memData = samples.map((s) => s.memory_used_percent ?? 0);
            const tempData = samples.map((s) => s.temperature_c ?? 0);
            const netRxData = samples.map((s) => (s.network?.eth0?.rx_bytes_per_sec ?? 0) + (s.network?.eth1?.rx_bytes_per_sec ?? 0) + (s.network?.wlan0?.rx_bytes_per_sec ?? 0));
            const netTxData = samples.map((s) => (s.network?.eth0?.tx_bytes_per_sec ?? 0) + (s.network?.eth1?.tx_bytes_per_sec ?? 0) + (s.network?.wlan0?.tx_bytes_per_sec ?? 0));
            // Auto-scale: anchor min at 0, max = actual peak + 30% headroom (minimum 10 for %)
            const cpuMax = Math.max(10, ...cpuData) * 1.3;
            const memMax = Math.max(10, ...memData) * 1.3;
            drawSparkline(monitorShell.querySelector("[data-chart-svg=\"cpu\"]"), cpuData, { stroke: "#39d0c8", min: 0, max: cpuMax, fmt: (v) => `${v.toFixed(1)}%` });
            drawSparkline(monitorShell.querySelector("[data-chart-svg=\"memory\"]"), memData, { stroke: "#f0a64b", min: 0, max: memMax, fmt: (v) => `${v.toFixed(1)}%` });
            drawSparkline(monitorShell.querySelector("[data-chart-svg=\"temp\"]"), tempData, { stroke: "#62d39e", fmt: (v) => `${v.toFixed(1)}°C` });
            drawDualSparkline(monitorShell.querySelector("[data-chart-svg=\"network\"]"), netRxData, netTxData);
        };

        const refreshMonitorCurrent = async () => {
            try {
                const response = await fetch("/api/system/metrics");
                if (!response.ok) return;
                applyCurrentMetrics(await response.json());
            } catch (err) {
                console.warn("Failed to refresh system metrics", err);
            }
        };

        const refreshMonitorHistory = async () => {
            try {
                const response = await fetch("/api/system/metrics/history");
                if (!response.ok) return;
                applyHistoryMetrics(await response.json());
            } catch (err) {
                console.warn("Failed to refresh system metrics history", err);
            }
        };

        refreshMonitorCurrent();
        refreshMonitorHistory();
        window.setInterval(refreshMonitorCurrent, 5000);
        window.setInterval(refreshMonitorHistory, 30000);
    }

    const interfacesShell = document.querySelector("[data-interfaces-shell]");
    if (interfacesShell) {
        // ── Interface type panel switching ─────────────────────────────────────
        const ifaceTypeBtns = Array.from(interfacesShell.querySelectorAll("[data-iface-type]"));
        const ifacePanels = Array.from(interfacesShell.querySelectorAll("[data-iface-panel]"));

        const switchIfacePanel = (type) => {
            ifaceTypeBtns.forEach((btn) => {
                const isActive = btn.getAttribute("data-iface-type") === type;
                btn.classList.toggle("is-current", isActive);
                if (!btn.classList.contains("is-locked")) {
                    btn.classList.toggle("is-preview", !isActive);
                }
                btn.setAttribute("aria-pressed", isActive ? "true" : "false");
            });
            ifacePanels.forEach((panel) => {
                panel.style.display = panel.getAttribute("data-iface-panel") === type ? "" : "none";
            });
        };

        ifaceTypeBtns.forEach((btn) => {
            btn.addEventListener("click", () => switchIfacePanel(btn.getAttribute("data-iface-type")));
        });

        // ── RS485 port tab switching ───────────────────────────────────────────
        const rtuTabBtns = Array.from(interfacesShell.querySelectorAll("[data-rtu-tab-btn]"));
        const rtuPanes = Array.from(interfacesShell.querySelectorAll("[data-rtu-port]"));

        rtuTabBtns.forEach((btn) => {
            btn.addEventListener("click", () => {
                const portId = btn.getAttribute("data-rtu-tab-btn");
                rtuTabBtns.forEach((b) => b.classList.toggle("is-active", b === btn));
                rtuPanes.forEach((p) => {
                    p.style.display = p.getAttribute("data-rtu-port") === portId ? "" : "none";
                });
            });
        });

        // ── RS232 port tabs ────────────────────────────────────────────────────
        const tabBtns = Array.from(interfacesShell.querySelectorAll("[data-rs232-tab-btn]"));
        const portPanes = Array.from(interfacesShell.querySelectorAll("[data-rs232-port]"));

        const switchTab = (portId) => {
            tabBtns.forEach((btn) => {
                btn.classList.toggle("is-active", btn.getAttribute("data-rs232-tab-btn") === portId);
            });
            portPanes.forEach((pane) => {
                pane.style.display = pane.getAttribute("data-rs232-port") === portId ? "" : "none";
            });
        };

        tabBtns.forEach((btn) => {
            btn.addEventListener("click", () => switchTab(btn.getAttribute("data-rs232-tab-btn")));
        });

        // ── Enable toggle per port ─────────────────────────────────────────────
        portPanes.forEach((pane) => {
            const portId = pane.getAttribute("data-rs232-port");
            const enableToggle = pane.querySelector("[data-rs232-enable]");
            const portBody = pane.querySelector("[data-rs232-port-body]");
            const disabledNote = pane.querySelector("[data-rs232-disabled-note]");
            const statusBadge = pane.querySelector(".iface-port-status");
            const tabDot = interfacesShell.querySelector(`[data-rs232-tab-dot="${portId}"]`);

            const applyPortState = (enabled) => {
                if (portBody) portBody.style.display = enabled ? "" : "none";
                if (disabledNote) disabledNote.style.display = enabled ? "none" : "";
                if (statusBadge) {
                    statusBadge.className = `iface-port-status ${enabled ? "is-active" : "is-idle"}`;
                    statusBadge.textContent = enabled ? "Active" : "Idle";
                }
                if (tabDot) {
                    tabDot.className = `iface-port-tab-dot ${enabled ? "is-active" : "is-idle"}`;
                }
            };

            if (enableToggle) {
                enableToggle.addEventListener("change", () => applyPortState(enableToggle.checked));
            }

            // ── Alarm accordion per port ───────────────────────────────────────
            pane.querySelectorAll("[data-alarm-toggle]").forEach((toggleBtn) => {
                const row = toggleBtn.closest("[data-alarm-ch]");
                const body = row?.querySelector("[data-alarm-body]");
                toggleBtn.addEventListener("click", () => {
                    const open = row.classList.toggle("is-open");
                    if (body) body.style.display = open ? "" : "none";
                });
            });

            // ── Analog output state → enable/disable channel select ────────────
            const analogStateSelect = pane.querySelector("[data-rs232-analog='state']");
            const analogChannelGroup = pane.querySelector("[data-analog-channel-group]");
            const analogChannelSelect = pane.querySelector("[data-rs232-analog='channel']");

            if (analogStateSelect) {
                const applyAnalogState = (state) => {
                    const off = state === "off";
                    if (analogChannelGroup) {
                        analogChannelGroup.style.opacity = off ? "0.45" : "";
                        analogChannelGroup.style.pointerEvents = off ? "none" : "";
                    }
                    if (analogChannelSelect) analogChannelSelect.disabled = off;
                };
                analogStateSelect.addEventListener("change", () => applyAnalogState(analogStateSelect.value));
            }
        });

        // ── Payload builder ────────────────────────────────────────────────────
        const buildPayload = () => {
            const buildPort = (portId) => {
                const pane = interfacesShell.querySelector(`[data-rs232-port="${portId}"]`);
                if (!pane) return null;

                const enabled = pane.querySelector("[data-rs232-enable]")?.checked ?? false;

                // Serial
                const serial = {};
                pane.querySelectorAll("[data-rs232-serial]").forEach((el) => {
                    const key = el.getAttribute("data-rs232-serial");
                    serial[key] = ["baud_rate", "stop_bits", "data_bits"].includes(key)
                        ? parseInt(el.value, 10)
                        : el.value;
                });

                // Polling
                const polling = {};
                pane.querySelectorAll("[data-rs232-poll]").forEach((el) => {
                    polling[el.getAttribute("data-rs232-poll")] = el.checked;
                });

                // Driver
                const driver = {};
                pane.querySelectorAll("[data-rs232-driver]").forEach((el) => {
                    driver[el.getAttribute("data-rs232-driver")] = el.checked;
                });

                // Alarms: collect by channel
                const alarms = {};
                pane.querySelectorAll("[data-rs232-alarm]").forEach((el) => {
                    const ch = el.getAttribute("data-rs232-alarm");
                    const field = el.getAttribute("data-rs232-alarm-field");
                    if (!alarms[ch]) alarms[ch] = {};
                    if (el.type === "checkbox") {
                        alarms[ch][field] = el.checked;
                    } else if (el.type === "number") {
                        alarms[ch][field] = parseFloat(el.value) || 0;
                    } else {
                        alarms[ch][field] = el.value;
                    }
                });

                // Analog output
                const analog = {};
                pane.querySelectorAll("[data-rs232-analog]").forEach((el) => {
                    const key = el.getAttribute("data-rs232-analog");
                    analog[key] = el.type === "number" ? parseFloat(el.value) || 0 : el.value;
                });
                if (analog.state === "off") analog.channel = null;

                return {
                    enabled,
                    serial,
                    sensor: "dustrak",
                    dustrak: { polling, driver, alarms, analog_output: analog },
                };
            };

            return {
                version: 1,
                rs232: { port_0: buildPort("0"), port_1: buildPort("1") },
            };
        };

        // ── Save button ────────────────────────────────────────────────────────
        const saveBtn = interfacesShell.querySelector("[data-iface-save]");
        const saveMessage = interfacesShell.querySelector("[data-iface-save-message]");

        if (saveBtn) {
            saveBtn.addEventListener("click", async () => {
                if (saveMessage) {
                    saveMessage.textContent = "";
                    saveMessage.classList.remove("is-success");
                }
                saveBtn.disabled = true;
                saveBtn.textContent = "Saving…";
                try {
                    const response = await fetch("/api/interfaces/rs232/config", {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify(buildPayload()),
                    });
                    const data = await response.json();
                    if (saveMessage) {
                        saveMessage.textContent = data.message || (response.ok ? "Saved." : "Save failed.");
                        saveMessage.classList.toggle("is-success", response.ok && data.ok);
                    }
                } catch {
                    if (saveMessage) saveMessage.textContent = "Could not reach the gateway.";
                } finally {
                    saveBtn.disabled = false;
                    saveBtn.textContent = "Save Interface Configuration";
                }
            });
        }

        // ── Shared: register table add/delete row ──────────────────────────────
        const REG_ROW_HTML = () => `
            <tr data-reg-row>
                <td><input type="text" class="iface-number iface-reg-input" data-reg-field="name" placeholder="e.g. Temperature"></td>
                <td><select class="iface-select iface-select-sm" data-reg-field="register_type">
                    <option value="coil">Coil (0x)</option>
                    <option value="discrete_input">Discrete Input (1x)</option>
                    <option value="input_register">Input Reg (3x)</option>
                    <option value="holding_register" selected>Holding Reg (4x)</option>
                </select></td>
                <td><input type="number" class="iface-number iface-reg-input" data-reg-field="address" min="0" max="65535" value="40001"></td>
                <td><select class="iface-select iface-select-sm" data-reg-field="data_type">
                    <option value="uint16" selected>UInt16</option>
                    <option value="int16">Int16</option>
                    <option value="uint32">UInt32</option>
                    <option value="int32">Int32</option>
                    <option value="float32">Float32</option>
                    <option value="bool">Bool</option>
                </select></td>
                <td><select class="iface-select iface-select-sm" data-reg-field="word_order">
                    <option value="big" selected>Big</option>
                    <option value="little">Little</option>
                </select></td>
                <td><input type="number" class="iface-number iface-reg-input" data-reg-field="scale" step="any" value="1"></td>
                <td><input type="text" class="iface-number iface-reg-input" data-reg-field="unit" placeholder="°C"></td>
                <td><button type="button" class="iface-reg-delete-btn" data-reg-delete title="Remove">✕</button></td>
            </tr>`;

        const wireRegTable = (container) => {
            const tbody = container.querySelector("[data-reg-tbody]");
            const addBtn = container.querySelector("[data-reg-add]");
            const empty = container.querySelector("[data-reg-empty]");

            const refreshEmpty = () => {
                const hasRows = tbody.querySelectorAll("[data-reg-row]").length > 0;
                if (empty) empty.style.display = hasRows ? "none" : "";
            };

            if (addBtn) {
                addBtn.addEventListener("click", () => {
                    const tmp = document.createElement("tbody");
                    tmp.innerHTML = REG_ROW_HTML();
                    const row = tmp.firstElementChild;
                    row.querySelector("[data-reg-delete]").addEventListener("click", () => {
                        row.remove(); refreshEmpty();
                    });
                    tbody.appendChild(row);
                    refreshEmpty();
                });
            }

            tbody.querySelectorAll("[data-reg-delete]").forEach((btn) => {
                btn.addEventListener("click", () => { btn.closest("[data-reg-row]").remove(); refreshEmpty(); });
            });
            refreshEmpty();
        };

        const readRegTable = (container) => {
            return Array.from(container.querySelectorAll("[data-reg-row]")).map((row) => {
                const f = (attr) => row.querySelector(`[data-reg-field="${attr}"]`)?.value ?? "";
                return {
                    name: f("name"),
                    register_type: f("register_type"),
                    address: parseInt(f("address"), 10) || 0,
                    data_type: f("data_type"),
                    word_order: f("word_order"),
                    scale: parseFloat(f("scale")) || 1,
                    unit: f("unit"),
                };
            });
        };

        // ── RS485 panel ────────────────────────────────────────────────────────
        const rs485Panel = interfacesShell.querySelector("[data-iface-panel='rs485']");
        if (rs485Panel) {
            // Wire register tables
            rs485Panel.querySelectorAll("[data-reg-table]").forEach(wireRegTable);

            // Enable toggles
            rs485Panel.querySelectorAll("[data-rtu-port]").forEach((pane) => {
                const portId = pane.getAttribute("data-rtu-port");
                const toggle = pane.querySelector("[data-rtu-enable]");
                const body = pane.querySelector("[data-rtu-port-body]");
                const note = pane.querySelector("[data-rtu-disabled-note]");
                const status = pane.querySelector(".iface-port-status");
                const dot = rs485Panel.querySelector(`[data-rtu-tab-dot="${portId}"]`);

                const apply = (enabled) => {
                    if (body) body.style.display = enabled ? "" : "none";
                    if (note) note.style.display = enabled ? "none" : "";
                    if (status) { status.className = `iface-port-status ${enabled ? "is-active" : "is-idle"}`; status.textContent = enabled ? "Active" : "Idle"; }
                    if (dot) dot.className = `iface-port-tab-dot ${enabled ? "is-active" : "is-idle"}`;
                };

                if (toggle) toggle.addEventListener("change", () => apply(toggle.checked));
            });

            // Save
            const rtuSaveBtn = rs485Panel.querySelector("[data-rtu-save]");
            const rtuSaveMsg = rs485Panel.querySelector("[data-rtu-save-message]");

            const buildRtuPayload = () => {
                const buildPort = (portId, portKey) => {
                    const pane = rs485Panel.querySelector(`[data-rtu-port="${portId}"]`);
                    if (!pane) return null;
                    const serial = {};
                    pane.querySelectorAll("[data-rtu-serial]").forEach((el) => {
                        const k = el.getAttribute("data-rtu-serial");
                        serial[k] = ["baud_rate", "stop_bits", "data_bits"].includes(k) ? parseInt(el.value, 10) : el.value;
                    });
                    const modbus_rtu = { registers: readRegTable(pane.querySelector("[data-reg-table]")) };
                    pane.querySelectorAll("[data-rtu-modbus]").forEach((el) => {
                        const k = el.getAttribute("data-rtu-modbus");
                        modbus_rtu[k] = parseInt(el.value, 10) || 0;
                    });
                    return {
                        enabled: pane.querySelector("[data-rtu-enable]")?.checked ?? false,
                        serial,
                        modbus_rtu,
                    };
                };
                return { version: 1, rs485: { port_2: buildPort("0"), port_3: buildPort("1") } };
            };

            if (rtuSaveBtn) {
                rtuSaveBtn.addEventListener("click", async () => {
                    if (rtuSaveMsg) { rtuSaveMsg.textContent = ""; rtuSaveMsg.classList.remove("is-success"); }
                    rtuSaveBtn.disabled = true; rtuSaveBtn.textContent = "Saving…";
                    try {
                        const res = await fetch("/api/interfaces/rs485/config", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(buildRtuPayload()) });
                        const data = await res.json();
                        if (rtuSaveMsg) { rtuSaveMsg.textContent = data.message || (res.ok ? "Saved." : "Save failed."); rtuSaveMsg.classList.toggle("is-success", res.ok && data.ok); }
                    } catch { if (rtuSaveMsg) rtuSaveMsg.textContent = "Could not reach the gateway."; }
                    finally { rtuSaveBtn.disabled = false; rtuSaveBtn.textContent = "Save RS485 Configuration"; }
                });
            }
        }

        // ── Modbus TCP panel ───────────────────────────────────────────────────
        const mtcpPanel = interfacesShell.querySelector("[data-iface-panel='modbus-tcp']");
        if (mtcpPanel) {
            const mtcpList = mtcpPanel.querySelector("[data-mtcp-list]");
            const mtcpCount = mtcpPanel.querySelector("[data-mtcp-count]");
            const MAX_CONN = 10;

            const updateMtcpCount = () => {
                const n = mtcpList.querySelectorAll("[data-mtcp-conn]").length;
                if (mtcpCount) mtcpCount.textContent = `${n} / ${MAX_CONN}`;
                const addBtn = mtcpPanel.querySelector("[data-mtcp-add-conn]");
                if (addBtn) addBtn.disabled = n >= MAX_CONN;
            };

            const wireMtcpConn = (connEl) => {
                // Accordion toggle
                const header = connEl.querySelector("[data-mtcp-conn-toggle]");
                const body = connEl.querySelector("[data-mtcp-conn-body]");
                const chevron = connEl.querySelector(".iface-alarm-chevron");
                if (header) header.addEventListener("click", () => {
                    const open = connEl.classList.toggle("is-open");
                    if (body) body.style.display = open ? "" : "none";
                    if (chevron) chevron.style.transform = open ? "rotate(90deg)" : "";
                });

                // Live summary update
                const nameInput = connEl.querySelector("[data-mtcp-field='name']");
                const ifaceSelect = connEl.querySelector("[data-mtcp-field='interface']");
                const ipInput = connEl.querySelector("[data-mtcp-field='ip']");
                const portInput = connEl.querySelector("[data-mtcp-field='port']");
                const unitInput = connEl.querySelector("[data-mtcp-field='unit_id']");
                const nameLabel = connEl.querySelector("[data-mtcp-conn-name-label]");
                const summary = connEl.querySelector("[data-mtcp-conn-summary]");
                const dot = connEl.querySelector("[data-mtcp-conn-dot]");
                const enableToggle = connEl.querySelector("[data-mtcp-conn-enable]");

                const refreshSummary = () => {
                    if (nameLabel && nameInput) nameLabel.textContent = nameInput.value || "Unnamed";
                    if (summary) summary.textContent = `${ifaceSelect?.value ?? "eth0"} · ${ipInput?.value || "—"}:${portInput?.value || "502"} · Unit ${unitInput?.value || "1"}`;
                };
                const refreshDot = () => {
                    if (dot) dot.className = `iface-conn-status-dot ${enableToggle?.checked ? "is-active" : "is-idle"}`;
                };

                [nameInput, ifaceSelect, ipInput, portInput, unitInput].forEach((el) => el?.addEventListener("input", refreshSummary));
                enableToggle?.addEventListener("change", refreshDot);

                // Delete connection
                connEl.querySelector("[data-mtcp-del-conn]")?.addEventListener("click", () => {
                    connEl.remove();
                    updateMtcpCount();
                    const empty = mtcpList.querySelector("[data-mtcp-empty]");
                    if (empty) empty.style.display = mtcpList.querySelectorAll("[data-mtcp-conn]").length === 0 ? "" : "none";
                });

                // Wire register table
                const regTable = connEl.querySelector("[data-reg-table]");
                if (regTable) wireRegTable(regTable);
            };

            // Wire existing connections
            mtcpList.querySelectorAll("[data-mtcp-conn]").forEach(wireMtcpConn);

            // Add connection
            const addConnBtn = mtcpPanel.querySelector("[data-mtcp-add-conn]");
            if (addConnBtn) {
                addConnBtn.addEventListener("click", () => {
                    if (mtcpList.querySelectorAll("[data-mtcp-conn]").length >= MAX_CONN) return;
                    const empty = mtcpList.querySelector("[data-mtcp-empty]");
                    if (empty) empty.style.display = "none";
                    const id = `conn_${Date.now()}`;
                    const tmpl = document.createElement("template");
                    tmpl.innerHTML = `
<div class="iface-alarm-row is-open" data-mtcp-conn>
    <button type="button" class="iface-alarm-header" data-mtcp-conn-toggle>
        <span class="iface-conn-status-dot is-idle" data-mtcp-conn-dot></span>
        <span class="iface-alarm-ch-label" style="min-width:auto;flex:1" data-mtcp-conn-name-label>New Device</span>
        <span class="iface-alarm-summary" data-mtcp-conn-summary>eth0 · —:502 · Unit 1</span>
        <span class="iface-alarm-chevron" style="transform:rotate(90deg)">▸</span>
    </button>
    <div class="iface-alarm-body" data-mtcp-conn-body style="display:block">
        <div class="iface-modbus-tcp-grid" style="margin-bottom:1rem">
            <label class="iface-field-group"><span class="iface-field-label">Name</span><input type="text" class="iface-number" data-mtcp-field="name" value="New Device"></label>
            <label class="iface-field-group"><span class="iface-field-label">Enabled</span><label class="iface-toggle" style="margin-top:0.35rem"><input type="checkbox" class="iface-toggle-input" data-mtcp-conn-enable><span class="iface-toggle-track" aria-hidden="true"></span></label></label>
            <label class="iface-field-group"><span class="iface-field-label">Ethernet Interface</span><select class="iface-select" data-mtcp-field="interface"><option value="eth0" selected>eth0 — Primary</option><option value="eth1">eth1 — Secondary</option></select></label>
            <label class="iface-field-group"><span class="iface-field-label">Device IP</span><input type="text" class="iface-number" data-mtcp-field="ip" placeholder="192.168.1.100"></label>
            <label class="iface-field-group"><span class="iface-field-label">Port</span><input type="number" class="iface-number" min="1" max="65535" data-mtcp-field="port" value="502"></label>
            <label class="iface-field-group"><span class="iface-field-label">Unit ID (1–247)</span><input type="number" class="iface-number" min="1" max="247" data-mtcp-field="unit_id" value="1"></label>
            <label class="iface-field-group"><span class="iface-field-label">Poll Interval</span><select class="iface-select" data-mtcp-field="poll_interval_ms"><option value="500">500 ms</option><option value="1000" selected>1 s</option><option value="2000">2 s</option><option value="5000">5 s</option><option value="10000">10 s</option></select></label>
        </div>
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.5rem">
            <p class="iface-section-eyebrow" style="margin:0">Register Map</p>
            <button type="button" class="iface-reg-delete-btn" style="padding:0.3rem 0.8rem;border-radius:0.5rem" data-mtcp-del-conn>Remove Connection</button>
        </div>
        <div data-reg-table><table class="iface-register-table"><thead><tr><th>Name</th><th>Register Type</th><th>Address</th><th>Data Type</th><th>Word Order</th><th>Scale</th><th>Unit</th><th></th></tr></thead><tbody data-reg-tbody><tr class="iface-register-empty" data-reg-empty><td colspan="8">No registers defined — click Add Register to start</td></tr></tbody></table><button type="button" class="iface-add-register-btn" data-reg-add>+ Add Register</button></div>
    </div>
</div>`;
                    const connEl = tmpl.content.firstElementChild;
                    mtcpList.appendChild(connEl);
                    wireMtcpConn(connEl);
                    updateMtcpCount();
                });
            }

            // Save
            const mtcpSaveBtn = mtcpPanel.querySelector("[data-mtcp-save]");
            const mtcpSaveMsg = mtcpPanel.querySelector("[data-mtcp-save-message]");

            const buildMtcpPayload = () => {
                const connections = Array.from(mtcpList.querySelectorAll("[data-mtcp-conn]")).map((connEl, i) => {
                    const f = (attr) => connEl.querySelector(`[data-mtcp-field="${attr}"]`)?.value ?? "";
                    return {
                        id: `conn_${i + 1}`,
                        name: f("name") || "Unnamed Device",
                        enabled: connEl.querySelector("[data-mtcp-conn-enable]")?.checked ?? false,
                        interface: f("interface") || "eth0",
                        ip: f("ip"),
                        port: parseInt(f("port"), 10) || 502,
                        unit_id: parseInt(f("unit_id"), 10) || 1,
                        poll_interval_ms: parseInt(f("poll_interval_ms"), 10) || 1000,
                        registers: readRegTable(connEl.querySelector("[data-reg-table]")),
                    };
                });
                return { version: 1, max_connections: MAX_CONN, connections };
            };

            if (mtcpSaveBtn) {
                mtcpSaveBtn.addEventListener("click", async () => {
                    if (mtcpSaveMsg) { mtcpSaveMsg.textContent = ""; mtcpSaveMsg.classList.remove("is-success"); }
                    mtcpSaveBtn.disabled = true; mtcpSaveBtn.textContent = "Saving…";
                    try {
                        const res = await fetch("/api/interfaces/modbus-tcp/config", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(buildMtcpPayload()) });
                        const data = await res.json();
                        if (mtcpSaveMsg) { mtcpSaveMsg.textContent = data.message || (res.ok ? "Saved." : "Save failed."); mtcpSaveMsg.classList.toggle("is-success", res.ok && data.ok); }
                    } catch { if (mtcpSaveMsg) mtcpSaveMsg.textContent = "Could not reach the gateway."; }
                    finally { mtcpSaveBtn.disabled = false; mtcpSaveBtn.textContent = "Save Modbus TCP Configuration"; }
                });
            }
        }
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

    // ══════════════════════════════════════════════════════════════════════
    //  Insights — IoT sensor analytics dashboard
    // ══════════════════════════════════════════════════════════════════════
    const insightsShell = document.querySelector("[data-insights-shell]");
    if (insightsShell) {

        // ── State ────────────────────────────────────────────────────────
        let idsConfigured    = [];           // static manifest from /api/insights/configured
        let idsHistSrc       = null;
        let idsHistDevId     = null;
        let idsHistMetric    = null;
        let idsHistWindow    = 6;
        let idsEventFilter   = "all";
        let idsAllEvents     = [];

        // ── DOM refs ─────────────────────────────────────────────────────
        const idsSensorGrid   = insightsShell.querySelector("[data-ids-sensor-grid]");
        const idsNoSensors    = insightsShell.querySelector("[data-ids-no-sensors]");
        const idsHistDrawer   = insightsShell.querySelector("[data-ids-history-drawer]");
        const idsHistChart    = insightsShell.querySelector("[data-ids-history-chart]");
        const idsEventList    = insightsShell.querySelector("[data-ids-event-list]");
        const idsFilterBar    = insightsShell.querySelector(".ids-filter-bar");
        const idsWindowSel    = insightsShell.querySelector("[data-ids-window-sel]");

        // ── Helpers ──────────────────────────────────────────────────────
        const fmtAge = (s) => {
            if (s < 5)    return "just now";
            if (s < 60)   return `${Math.round(s)}s ago`;
            if (s < 3600) return `${Math.round(s / 60)}m ago`;
            return `${Math.round(s / 3600)}h ago`;
        };

        const fmtTs = (ms) =>
            new Date(ms).toLocaleTimeString([], { hour12: false, hour: "2-digit", minute: "2-digit", second: "2-digit" });

        const fmtDate = (ms) =>
            new Date(ms).toLocaleDateString([], { month: "short", day: "numeric" });

        const fmtNum = (v, decimals = 3) => {
            if (v === null || v === undefined) return "--";
            if (typeof v !== "number") return String(v);
            return Number.isInteger(v) ? String(v) : v.toFixed(decimals);
        };

        const tpLabel = (tp) => {
            if (!tp) return "";
            if (tp.type === "serial")     return tp.endpoint || "";
            if (tp.type === "modbus_rtu") return `${tp.endpoint || ""} · slave ${tp.slave_address || ""}`.trimEnd().replace(/ · $/, "");
            if (tp.type === "modbus_tcp") return `${tp.endpoint || ""}:${tp.port || 502} · ${tp.interface || "eth0"}`;
            return tp.endpoint || "";
        };

        // ── Sparkline ────────────────────────────────────────────────────
        const drawSparkline = (svgEl, values) => {
            if (!svgEl) return;
            const W = 90, H = 28, pad = 2;
            const uW = W - pad * 2, uH = H - pad * 2;

            if (!values || values.length < 2) {
                svgEl.innerHTML = `<line x1="0" y1="${H / 2}" x2="${W}" y2="${H / 2}" stroke="rgba(255,255,255,0.1)" stroke-dasharray="2,3"/>`;
                return;
            }

            const min   = Math.min(...values);
            const max   = Math.max(...values);
            const range = max - min || 1;
            const step  = uW / (values.length - 1);
            const toX   = (i) => pad + i * step;
            const toY   = (v) => pad + uH - ((v - min) / range) * uH;

            const path = values.map((v, i) => `${i === 0 ? "M" : "L"} ${toX(i).toFixed(1)},${toY(v).toFixed(1)}`).join(" ");
            svgEl.innerHTML = `<path d="${path}" fill="none" stroke="rgba(57,208,200,0.72)" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>`;
        };

        // ── KPI strip ────────────────────────────────────────────────────
        const updateKPIs = async () => {
            try {
                const r = await fetch("/api/insights/summary");
                const d = await r.json();
                if (!d.ok) return;

                const kpi    = (key) => insightsShell.querySelector(`[data-ins-kpi="${key}"]`);
                const kpiSub = (key) => insightsShell.querySelector(`[data-ins-kpi-sub="${key}"]`);

                const devEl = kpi("devices");
                if (devEl) devEl.textContent = d.active_devices ?? "--";
                const devSub = kpiSub("devices");
                if (devSub) devSub.textContent = d.active_devices === 0 ? "No live sensors" : `of ${idsConfigured.length} configured`;

                const qEl = kpi("quality");
                if (qEl) qEl.textContent = `${d.quality_pct ?? "--"}%`;
                const qBar = insightsShell.querySelector(`[data-ins-kpi-bar="quality"]`);
                if (qBar) {
                    const pct = d.quality_pct ?? 0;
                    qBar.style.width = `${pct}%`;
                    qBar.classList.toggle("is-warm", pct < 90 && pct >= 70);
                    qBar.classList.toggle("is-hot",  pct < 70);
                }

                const anEl  = kpi("anomalies");
                if (anEl)  anEl.textContent  = d.anomaly_count ?? "--";
                const anSub = kpiSub("anomalies");
                if (anSub) anSub.textContent = d.anomaly_count === 0 ? "All systems nominal" : "Gaps or errors detected";

                const evEl  = kpi("last-event");
                const evSub = kpiSub("last-event");
                if (d.last_event_ms) {
                    const age = (Date.now() - d.last_event_ms) / 1000;
                    if (evEl)  evEl.textContent  = fmtAge(age);
                    if (evSub) evSub.textContent = `${fmtDate(d.last_event_ms)} ${fmtTs(d.last_event_ms)}`;
                }
            } catch (_) { /* silent */ }
        };

        // ── Step 1: Load configured devices and build card skeletons ─────
        const buildCardSkeleton = (device) => {
            const key    = `${device.source}:${device.device_id}`;
            const tpStr  = tpLabel(device.transport);
            const type   = device.device_type || device.source || "sensor";

            const metricsHTML = (device.expected_metrics || []).map((m) => `
                <button class="ids-metric-row"
                    data-ids-metric-btn
                    data-source="${device.source}"
                    data-device-id="${device.device_id}"
                    data-metric="${m.name}">
                    <div class="ids-metric-info">
                        <span class="ids-metric-label">${m.name.toUpperCase()}</span>
                        <div class="ids-metric-reading">
                            <span class="ids-metric-val" data-ids-val="${m.name}">--</span>
                            <span class="ids-metric-unit">${m.unit || ""}</span>
                        </div>
                    </div>
                    <div class="ids-metric-chart-col">
                        <svg class="ids-sparkline" data-ids-spark="${m.name}"
                             viewBox="0 0 90 28" preserveAspectRatio="none">
                            <line x1="0" y1="14" x2="90" y2="14"
                                  stroke="rgba(255,255,255,0.1)" stroke-dasharray="2,3"/>
                        </svg>
                        <span class="ids-quality-dot" data-ids-quality="${m.name}"></span>
                    </div>
                </button>`).join("");

            return `
                <article class="ids-card" data-ids-card="${key}">
                    <header class="ids-card-head">
                        <div>
                            <p class="ids-card-name">${device.name}</p>
                            <div class="ids-card-chips">
                                <span class="ids-chip ids-chip-type">${type}</span>
                                ${tpStr ? `<span class="ids-chip ids-chip-transport">${tpStr}</span>` : ""}
                            </div>
                        </div>
                        <span class="ids-card-status ids-status-awaiting" data-ids-card-status>
                            <span class="ids-status-pulse"></span>
                            <span class="ids-status-label">Awaiting</span>
                        </span>
                    </header>
                    <div class="ids-metric-list">${metricsHTML || "<p style='padding:1rem;color:var(--muted);font-size:.84rem'>No registers configured.</p>"}</div>
                    <footer class="ids-card-foot" data-ids-last-seen>Awaiting first sample&hellip;</footer>
                </article>`;
        };

        const loadConfigured = async () => {
            try {
                const r = await fetch("/api/insights/configured");
                const d = await r.json();
                if (!d.ok || !idsSensorGrid) return;

                idsConfigured = d.devices || [];

                if (idsConfigured.length === 0) {
                    idsNoSensors?.classList.remove("ids-hidden");
                    return;
                }

                idsNoSensors?.classList.add("ids-hidden");
                idsSensorGrid.innerHTML = idsConfigured.map(buildCardSkeleton).join("");
                wireMetricBtns();
            } catch (_) { /* silent */ }
        };

        const wireMetricBtns = () => {
            idsSensorGrid?.querySelectorAll("[data-ids-metric-btn]").forEach((btn) => {
                btn.addEventListener("click", () => {
                    openHistory(
                        btn.getAttribute("data-source"),
                        btn.getAttribute("data-device-id"),
                        btn.getAttribute("data-metric"),
                    );
                });
            });
        };

        // ── Step 2: Overlay live Redis data every 3s ─────────────────────
        const applyLiveData = (devices) => {
            if (!idsSensorGrid) return;

            const liveMap = {};
            for (const d of devices) {
                liveMap[`${d.source}:${d.device_id}`] = d;
            }

            idsSensorGrid.querySelectorAll("[data-ids-card]").forEach((card) => {
                const key        = card.getAttribute("data-ids-card");
                const live       = liveMap[key];
                const statusEl   = card.querySelector("[data-ids-card-status]");
                const statusLabel = statusEl?.querySelector(".ids-status-label");
                const lastSeenEl = card.querySelector("[data-ids-last-seen]");

                if (!live) {
                    if (statusEl) {
                        statusEl.className = "ids-card-status ids-status-offline";
                        if (statusLabel) statusLabel.textContent = "Offline";
                    }
                    return;
                }

                // Status badge
                const sc = live.status === "ok" ? "ids-status-live" :
                           live.status === "warning" ? "ids-status-warning" : "ids-status-error";
                const sl = live.status === "ok" ? "Live" :
                           live.status === "warning" ? "Warning" : "Error";
                if (statusEl) {
                    statusEl.className = `ids-card-status ${sc}`;
                    if (statusLabel) statusLabel.textContent = sl;
                }

                // Card border
                card.classList.toggle("ids-card-warning", live.status === "warning");
                card.classList.toggle("ids-card-error",   live.status === "error");

                // Last seen
                if (lastSeenEl && live.timestamp_ms) {
                    const age = (Date.now() - live.timestamp_ms) / 1000;
                    lastSeenEl.textContent = `Updated ${fmtAge(age)}`;
                }

                // Metrics
                const metrics = live.metrics  || {};
                const samples = live._samples || {};

                for (const [mKey, m] of Object.entries(metrics)) {
                    const valEl  = card.querySelector(`[data-ids-val="${mKey}"]`);
                    const sparkEl = card.querySelector(`[data-ids-spark="${mKey}"]`);
                    const qualEl = card.querySelector(`[data-ids-quality="${mKey}"]`);

                    if (valEl) {
                        const decimals = (m.value !== null && typeof m.value === "number" && !Number.isInteger(m.value)) ? 3 : 0;
                        valEl.textContent = fmtNum(m.value, decimals);
                    }

                    if (sparkEl) {
                        drawSparkline(sparkEl, samples[mKey] || []);
                    }

                    if (qualEl) {
                        const qc = m.quality === "good"  ? "ids-q-good"  :
                                   m.quality === "stale" ? "ids-q-stale" : "ids-q-error";
                        qualEl.className = `ids-quality-dot ${qc}`;
                    }
                }
            });
        };

        const loadLive = async () => {
            try {
                const r = await fetch("/api/insights/live");
                const d = await r.json();
                if (d.ok) applyLiveData(d.devices || []);
            } catch (_) { /* silent */ }
        };

        // ── History drawer ───────────────────────────────────────────────
        const openHistory = async (source, deviceId, metric) => {
            idsHistSrc    = source;
            idsHistDevId  = deviceId;
            idsHistMetric = metric;

            const devNameEl = idsHistDrawer?.querySelector("[data-ids-history-device]");
            const metricEl  = idsHistDrawer?.querySelector("[data-ids-history-metric]");

            const card    = idsSensorGrid?.querySelector(`[data-ids-card="${source}:${deviceId}"]`);
            const devName = card?.querySelector(".ids-card-name")?.textContent ?? deviceId;

            if (devNameEl) devNameEl.textContent = devName;
            if (metricEl)  metricEl.textContent  = metric;

            idsHistDrawer?.classList.remove("ids-hidden");
            idsHistDrawer?.scrollIntoView({ behavior: "smooth", block: "nearest" });

            await loadHistory();
        };

        const loadHistory = async () => {
            if (!idsHistSrc || !idsHistDevId || !idsHistMetric) return;

            const params = new URLSearchParams({
                source:    idsHistSrc,
                device_id: idsHistDevId,
                metric:    idsHistMetric,
                window:    String(idsHistWindow),
            });

            try {
                const r = await fetch(`/api/insights/history?${params}`);
                const d = await r.json();
                if (!d.ok) return;

                const ts  = d.timestamps || [];
                const avg = d.avg || [];
                const mn  = d.min || [];
                const mx  = d.max || [];
                const cnt = d.count || [];

                const setHStat = (key, val) => {
                    const el = idsHistDrawer?.querySelector(`[data-hstat="${key}"]`);
                    if (el) el.textContent = typeof val === "number" ? fmtNum(val, 3) : (val ?? "--");
                };

                const last  = avg.length  > 0 ? avg[avg.length - 1] : null;
                const mean  = avg.length  > 0 ? avg.reduce((a, b) => a + (b ?? 0), 0) / avg.length : null;
                const allMn = mn.length   > 0 ? Math.min(...mn.filter((v) => v !== null)) : null;
                const allMx = mx.length   > 0 ? Math.max(...mx.filter((v) => v !== null)) : null;
                const sampleCount = cnt.reduce((a, b) => a + b, 0);

                setHStat("current", last);
                setHStat("avg",     mean);
                setHStat("min",     allMn);
                setHStat("max",     allMx);
                setHStat("samples", sampleCount);

                if (ts.length === 0) {
                    if (idsHistChart) idsHistChart.innerHTML = `<p class="ids-empty-msg" style="width:100%;text-align:center">No history in this window. Data appears as sensors report.</p>`;
                    return;
                }

                drawHistoryChart(ts, avg, mn, mx);
            } catch (_) { /* silent */ }
        };

        const drawHistoryChart = (ts, avgVals, minVals, maxVals) => {
            if (!idsHistChart) return;
            const W   = idsHistChart.clientWidth || 600;
            const H   = 130;
            const pad = { t: 14, r: 16, b: 30, l: 44 };
            const uW  = W - pad.l - pad.r;
            const uH  = H - pad.t - pad.b;
            const n   = ts.length;

            const allV = [...avgVals, ...minVals, ...maxVals].filter((v) => v !== null);
            const minV = allV.length > 0 ? Math.min(...allV) : 0;
            const maxV = allV.length > 0 ? Math.max(...allV) : 1;
            const rng  = maxV - minV || 1;

            const toX = (i) => pad.l + (i / Math.max(1, n - 1)) * uW;
            const toY = (v) => pad.t + uH - ((v - minV) / rng) * uH;
            const safe = (arr, i) => (arr[i] !== null && arr[i] !== undefined) ? arr[i] : (minV + maxV) / 2;

            const bandTop = maxVals.map((_, i) => `${i === 0 ? "M" : "L"} ${toX(i).toFixed(1)},${toY(safe(maxVals, i)).toFixed(1)}`).join(" ");
            const bandBot = minVals.slice().reverse().map((_, i) => `L ${toX(n - 1 - i).toFixed(1)},${toY(safe(minVals, n - 1 - i)).toFixed(1)}`).join(" ");
            const avgPath = avgVals.map((_, i) => `${i === 0 ? "M" : "L"} ${toX(i).toFixed(1)},${toY(safe(avgVals, i)).toFixed(1)}`).join(" ");

            const labelStep = Math.max(1, Math.floor(n / 5));
            const tLabels = ts.filter((_, i) => i % labelStep === 0 || i === n - 1).map((t, li, arr) => {
                const origI = li === arr.length - 1 ? n - 1 : li * labelStep;
                return `<text x="${toX(origI).toFixed(1)}" y="${H - 5}" text-anchor="middle" class="ids-chart-axis">${fmtTs(t)}</text>`;
            }).join("");

            const yLabels = [0, 0.5, 1].map((f) => {
                const v = minV + f * rng;
                return `<text x="${(pad.l - 5).toFixed(1)}" y="${toY(v).toFixed(1)}" text-anchor="end" dominant-baseline="middle" class="ids-chart-axis">${v.toFixed(2)}</text>`;
            }).join("");

            idsHistChart.innerHTML = `
                <svg viewBox="0 0 ${W} ${H}" width="100%" height="${H}">
                    <path d="${bandTop} ${bandBot} Z" fill="rgba(57,208,200,0.1)" stroke="none"/>
                    <path d="${avgPath}" fill="none" stroke="rgba(57,208,200,0.85)" stroke-width="1.8"
                          stroke-linecap="round" stroke-linejoin="round"/>
                    ${tLabels}
                    ${yLabels}
                </svg>`;
        };

        // History controls
        idsHistDrawer?.querySelector("[data-ids-history-close]")?.addEventListener("click", () => {
            idsHistDrawer.classList.add("ids-hidden");
        });

        idsWindowSel?.querySelectorAll(".ids-window-btn").forEach((btn) => {
            btn.addEventListener("click", () => {
                idsWindowSel.querySelectorAll(".ids-window-btn").forEach((b) => b.classList.remove("ids-current"));
                btn.classList.add("ids-current");
                idsHistWindow = parseInt(btn.getAttribute("data-window"), 10);
                loadHistory();
            });
        });

        // ── Event log ────────────────────────────────────────────────────
        const renderEvents = () => {
            if (!idsEventList) return;
            const shown = idsEventFilter === "all"
                ? idsAllEvents
                : idsAllEvents.filter((e) => e.severity === idsEventFilter);

            if (shown.length === 0) {
                idsEventList.innerHTML = `<p class="ids-empty-msg">${
                    idsAllEvents.length === 0
                        ? "No events recorded — sensors reporting clean."
                        : `No ${idsEventFilter} events found.`
                }</p>`;
                return;
            }

            idsEventList.innerHTML = shown.map((ev) => {
                const cls     = `ids-sev-${ev.severity || "info"}`;
                const devName = ev.device_name || ev.device_id || "--";
                const ts      = ev.timestamp_ms ? `${fmtDate(ev.timestamp_ms)} ${fmtTs(ev.timestamp_ms)}` : "--";
                return `
                    <div class="ids-event-item ${cls}" data-severity="${ev.severity}">
                        <div class="ids-event-head">
                            <span class="ids-sev-dot"></span>
                            <span class="ids-ev-time">${ts}</span>
                            <span class="ids-ev-device">${devName}</span>
                            <span class="ids-ev-type">${ev.event_type || "--"}</span>
                            <span class="ids-ev-source">${ev.source || ""}</span>
                        </div>
                        <p class="ids-ev-msg">${ev.message || ""}</p>
                    </div>`;
            }).join("");
        };

        const loadEvents = async () => {
            try {
                const r = await fetch("/api/insights/events?limit=100");
                const d = await r.json();
                if (!d.ok) return;
                idsAllEvents = d.events || [];
                renderEvents();
            } catch (_) { /* silent */ }
        };

        // Filter buttons
        idsFilterBar?.querySelectorAll("[data-ids-filter]").forEach((btn) => {
            btn.addEventListener("click", () => {
                idsFilterBar.querySelectorAll("[data-ids-filter]").forEach((b) => b.classList.remove("ids-current"));
                btn.classList.add("ids-current");
                idsEventFilter = btn.getAttribute("data-ids-filter");
                renderEvents();
            });
        });

        // ── Bootstrap ────────────────────────────────────────────────────
        loadConfigured().then(() => loadLive());
        updateKPIs();
        loadEvents();

        setInterval(() => { loadLive(); updateKPIs(); }, 3000);
        setInterval(loadEvents, 15000);
    }

});