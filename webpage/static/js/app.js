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

    if (loginForm) {
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
    }

    // ══════════════════════════════════════════════════════════════════════
    //  Insights — Overview Command Dashboard
    // ══════════════════════════════════════════════════════════════════════
    const insightsShell = document.querySelector("[data-insights-shell]");
    console.log("[Insights] shell found:", !!insightsShell);
    if (insightsShell) {

        // ── State ────────────────────────────────────────────────────────
        let ovConfiguredCount = 0;   // total from /api/insights/configured
        let ovConfigured      = [];  // full device list (shared with Sensors tab)
        let ovLiveDevices     = [];  // latest from /api/insights/live
        let svRefreshFn       = null; // set by Sensors tab block
        let evRefreshFn       = null; // set by Events tab block

        // ── DOM refs ─────────────────────────────────────────────────────
        const ovDeviceGrid = insightsShell.querySelector("[data-ov-device-grid]");
        const ovNoSensors  = insightsShell.querySelector("[data-ov-no-sensors]");

        // ── Helpers ───────────────────────────────────────────────────────
        const fmtAge = (s) => {
            if (s < 5)    return "just now";
            if (s < 60)   return `${Math.round(s)}s ago`;
            if (s < 3600) return `${Math.round(s / 60)}m ago`;
            return `${Math.round(s / 3600)}h ago`;
        };

        const fmtVal = (v) => {
            if (v === null || v === undefined) return "--";
            if (typeof v !== "number") return String(v).trim();
            if (Number.isInteger(v)) return String(v);
            // Smart decimal: show enough significant digits without trailing zeros
            const abs = Math.abs(v);
            if (abs >= 100)  return v.toFixed(1);
            if (abs >= 10)   return v.toFixed(2);
            if (abs >= 1)    return v.toFixed(3);
            return v.toFixed(4);
        };

        // Known display names for common metric keys
        const METRIC_LABELS = {
            pm1: "PM1", pm25: "PM2.5", pm4: "PM4", pm10: "PM10", total: "Total PM",
        };

        const displayLabel = (rawName) => {
            const clean = (rawName || "").trim();
            return METRIC_LABELS[clean.toLowerCase()] ||
                clean.replace(/[_]+/g, " ")
                     .split(" ")
                     .map(w => w.charAt(0).toUpperCase() + w.slice(1))
                     .join(" ");
        };

        const tpStr = (tp) => {
            if (!tp) return "";
            if (tp.type === "serial")     return `${tp.endpoint || ""}`;
            if (tp.type === "modbus_rtu") return `${tp.endpoint || ""}`;
            if (tp.type === "modbus_tcp") return `${tp.endpoint || ""}:${tp.port || 502} · ${tp.interface || "eth0"}`;
            return tp.endpoint || "";
        };

        // ── Trend from sample array ────────────────────────────────────────
        const computeTrend = (values) => {
            if (!values || values.length < 10) return "flat";
            const recent  = values.slice(-5);
            const earlier = values.slice(-20, -15);
            if (earlier.length < 3) return "flat";
            const avg = (arr) => arr.reduce((a, b) => a + b, 0) / arr.length;
            const recentAvg  = avg(recent);
            const earlierAvg = avg(earlier);
            const base = Math.abs(earlierAvg) || 1;
            const pct  = (recentAvg - earlierAvg) / base * 100;
            if (pct > 3)  return "up";
            if (pct < -3) return "down";
            return "flat";
        };

        // ── Sparkline SVG ────────────────────────────────────────────────
        const drawSparkline = (svgEl, values, quality) => {
            if (!svgEl) return;
            const W = svgEl.clientWidth || 100;
            const H = 32;
            const pad = 3;
            const uH  = H - pad * 2;
            const color = quality === "good"  ? "rgba(57,208,200,0.72)" :
                          quality === "stale" ? "rgba(240,166,75,0.6)"  :
                                               "rgba(220,80,80,0.55)";

            if (!values || values.length < 2) {
                svgEl.innerHTML = `<line x1="0" y1="${H/2}" x2="${W}" y2="${H/2}" stroke="rgba(255,255,255,0.08)" stroke-dasharray="2,3"/>`;
                return;
            }

            const min = Math.min(...values);
            const max = Math.max(...values);
            const rng = max - min || 1;
            const step = (W - pad * 2) / (values.length - 1);
            const toX = (i) => pad + i * step;
            const toY = (v) => pad + uH - ((v - min) / rng) * uH;

            // Area fill
            const pathD = values.map((v, i) => `${i === 0 ? "M" : "L"} ${toX(i).toFixed(1)},${toY(v).toFixed(1)}`).join(" ");
            const areaD = `${pathD} L ${toX(values.length-1).toFixed(1)},${H} L ${pad},${H} Z`;

            svgEl.setAttribute("viewBox", `0 0 ${W} ${H}`);
            svgEl.innerHTML = `
                <path d="${areaD}" fill="${color.replace('0.72','0.1').replace('0.6','0.08').replace('0.55','0.07')}" stroke="none"/>
                <path d="${pathD}" fill="none" stroke="${color}" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>`;
        };

        // ── Build card skeleton from configured device ─────────────────────
        const buildCard = (device) => {
            const key     = `${device.source}:${device.device_id}`;
            const type    = device.device_type || device.source || "sensor";
            const tp      = tpStr(device.transport);
            const metrics = device.expected_metrics || [];

            const metricRows = metrics.map((m) => {
                const label = displayLabel(m.name);
                const unit  = (m.unit || "").trim();
                return `
                    <div class="ov-metric-row" data-ov-metric="${m.name}">
                        <span class="ov-metric-label" title="${label}">${label}</span>
                        <div class="ov-metric-reading">
                            <span class="ov-metric-val" data-ov-val>--</span>
                            <span class="ov-metric-unit">${unit}</span>
                            <span class="ov-trend" data-ov-trend></span>
                        </div>
                        <svg class="ov-sparkline" data-ov-spark height="32" preserveAspectRatio="none">
                            <line x1="0" y1="16" x2="100" y2="16" stroke="rgba(255,255,255,0.08)" stroke-dasharray="2,3"/>
                        </svg>
                        <span class="ov-quality-dot" data-ov-quality></span>
                    </div>`;
            }).join("");

            return `
                <article class="ov-card" data-ov-card="${key}">
                    <header class="ov-card-head">
                        <div>
                            <p class="ov-card-name">${device.name || device.device_id}</p>
                            <div class="ov-card-chips">
                                <span class="ov-chip ov-chip-type">${type}</span>
                                ${tp ? `<span class="ov-chip ov-chip-transport">${tp}</span>` : ""}
                            </div>
                        </div>
                        <span class="ov-status-badge ov-status-awaiting" data-ov-status>
                            <span class="ov-status-pulse"></span>
                            <span class="ov-status-text">Awaiting</span>
                        </span>
                    </header>
                    <div class="ov-card-issues" data-ov-issues hidden>
                        <span class="ov-card-issues-dot"></span>
                        <span class="ov-card-issues-text" data-ov-issues-text></span>
                    </div>
                    <div class="ov-metric-list">
                        ${metricRows || `<p style="padding:1rem;color:var(--muted);font-size:.84rem">No registers configured.</p>`}
                    </div>
                    <footer class="ov-card-foot">
                        <div class="ov-health-row">
                            <span class="ov-health-pct" data-ov-health-pct>-- health</span>
                            <span class="ov-last-seen" data-ov-last-seen></span>
                        </div>
                        <div class="ov-health-bar-track">
                            <div class="ov-health-bar-fill" data-ov-health-bar></div>
                        </div>
                    </footer>
                </article>`;
        };

        // ── Apply live data to existing card skeletons ─────────────────────
        const applyLiveToCards = () => {
            if (!ovDeviceGrid) return;

            const liveMap = {};
            for (const d of ovLiveDevices) {
                liveMap[`${d.source}:${d.device_id}`] = d;
            }

            ovDeviceGrid.querySelectorAll("[data-ov-card]").forEach((card) => {
                const key       = card.getAttribute("data-ov-card");
                const live      = liveMap[key];
                const status    = card.querySelector("[data-ov-status]");
                const stText    = status?.querySelector(".ov-status-text");
                const issuesEl  = card.querySelector("[data-ov-issues]");
                const issuesTx  = card.querySelector("[data-ov-issues-text]");

                if (!live) {
                    // Device configured but not live in Redis
                    if (status) {
                        status.className = "ov-status-badge ov-status-offline";
                        if (stText) stText.textContent = "Offline";
                    }
                    card.classList.remove("is-live", "is-warning", "is-error");
                    card.classList.add("is-offline");
                    if (issuesEl) issuesEl.hidden = true;
                    return;
                }

                // ── Status badge ──────────────────────────────────────────
                const devStatus = live.status || "ok";
                const sc = devStatus === "ok"      ? "ov-status-live"    :
                           devStatus === "warning"  ? "ov-status-warning" :
                                                     "ov-status-error";
                const sl = devStatus === "ok" ? "Live" :
                           devStatus === "warning" ? "Warning" : "Error";
                if (status) {
                    status.className = `ov-status-badge ${sc}`;
                    if (stText) stText.textContent = sl;
                }
                card.classList.remove("is-offline");
                card.classList.toggle("is-live",    devStatus === "ok");
                card.classList.toggle("is-warning", devStatus === "warning");
                card.classList.toggle("is-error",   devStatus === "error");

                // ── Last seen ─────────────────────────────────────────────
                const lastSeenEl = card.querySelector("[data-ov-last-seen]");
                if (lastSeenEl && live.timestamp_ms) {
                    lastSeenEl.textContent = fmtAge((Date.now() - live.timestamp_ms) / 1000);
                }

                // ── Metric rows ───────────────────────────────────────────
                const metrics  = live.metrics  || {};
                const samples  = live._samples || {};
                let goodCount  = 0;
                let totalCount = 0;
                let staleCount = 0;
                let errorCount = 0;

                card.querySelectorAll("[data-ov-metric]").forEach((row) => {
                    const mkey    = row.getAttribute("data-ov-metric");
                    const m       = metrics[mkey];
                    const valEl   = row.querySelector("[data-ov-val]");
                    const sparkEl = row.querySelector("[data-ov-spark]");
                    const qualEl  = row.querySelector("[data-ov-quality]");
                    const trendEl = row.querySelector("[data-ov-trend]");

                    if (!m) {
                        row.classList.remove("is-stale", "is-error");
                        return;
                    }

                    totalCount++;
                    const q = m.quality || "good";
                    if (q === "good")  goodCount++;
                    if (q === "stale") staleCount++;
                    if (q === "error") errorCount++;

                    // Row tint
                    row.classList.toggle("is-stale", q === "stale");
                    row.classList.toggle("is-error", q === "error");

                    // Value
                    if (valEl) {
                        valEl.textContent = fmtVal(m.value);
                        valEl.className = `ov-metric-val${q === "stale" ? " is-stale" : q === "error" ? " is-error" : ""}`;
                    }

                    // Sparkline
                    if (sparkEl) {
                        drawSparkline(sparkEl, samples[mkey] || [], q);
                    }

                    // Trend arrow
                    if (trendEl) {
                        const trend = computeTrend(samples[mkey] || []);
                        trendEl.textContent = trend === "up" ? "↑" : trend === "down" ? "↓" : "";
                        trendEl.className   = `ov-trend ov-trend-${trend}`;
                    }

                    // Quality dot
                    if (qualEl) {
                        qualEl.className = `ov-quality-dot${q === "good" ? " is-good" : q === "stale" ? " is-stale" : " is-error"}`;
                    }
                });

                // ── Per-card issue banner ────────────────────────────────
                if (issuesEl && issuesTx) {
                    let txt = "";
                    let isErr = false;
                    if (live.error) {
                        txt = live.error.message || `${live.error.type || "device error"}`;
                        isErr = true;
                    } else if (errorCount > 0) {
                        txt = `${errorCount} reading${errorCount > 1 ? "s" : ""} errored`;
                        isErr = true;
                    } else if (staleCount > 0) {
                        txt = `${staleCount} reading${staleCount > 1 ? "s" : ""} stale`;
                        isErr = false;
                    }
                    if (txt) {
                        issuesTx.textContent = txt;
                        issuesEl.classList.toggle("is-error", isErr);
                        issuesEl.hidden = false;
                    } else {
                        issuesEl.hidden = true;
                    }
                }

                // ── Health bar ────────────────────────────────────────────
                const pct      = totalCount > 0 ? Math.round((goodCount / totalCount) * 100) : 100;
                const hPctEl   = card.querySelector("[data-ov-health-pct]");
                const hBarEl   = card.querySelector("[data-ov-health-bar]");
                if (hPctEl)  hPctEl.textContent  = `${pct}% health`;
                if (hBarEl) {
                    hBarEl.style.width = `${pct}%`;
                    hBarEl.className   = `ov-health-bar-fill${pct < 70 ? " is-crit" : pct < 90 ? " is-warn" : ""}`;
                }
            });
        };

        // ── KPI Strip ─────────────────────────────────────────────────────
        const updateKPIs = () => {
            const kpi    = (k) => insightsShell.querySelector(`[data-ov-kpi="${k}"]`);
            const kpiSub = (k) => insightsShell.querySelector(`[data-ov-kpi-sub="${k}"]`);

            const liveCount = ovLiveDevices.length;
            const devEl     = kpi("devices");
            if (devEl) devEl.textContent = String(liveCount);
            const devSub = kpiSub("devices");
            if (devSub) devSub.textContent = `of ${ovConfiguredCount} configured`;

            // Quality + health from live devices
            let totalMetrics = 0, goodMetrics = 0;
            for (const d of ovLiveDevices) {
                for (const m of Object.values(d.metrics || {})) {
                    totalMetrics++;
                    if (m.quality === "good") goodMetrics++;
                }
            }
            const qualPct   = totalMetrics > 0 ? Math.round((goodMetrics / totalMetrics) * 100) : (liveCount > 0 ? 100 : 0);
            const healthPct = qualPct;  // same calculation for now; will use SQLite rollup later

            const qEl = kpi("quality");
            if (qEl) qEl.textContent = liveCount > 0 ? `${qualPct}%` : "--%";
            const qBar = insightsShell.querySelector(`[data-ov-kpi-bar="quality"]`);
            if (qBar) {
                qBar.style.width  = `${qualPct}%`;
                qBar.className    = `ov-kpi-bar-fill${qualPct < 70 ? " is-hot" : qualPct < 90 ? " is-warm" : ""}`;
            }

            const hEl = kpi("health");
            if (hEl) hEl.textContent = liveCount > 0 ? `${healthPct}%` : "--%";
            const hBar = insightsShell.querySelector(`[data-ov-kpi-bar="health"]`);
            if (hBar) {
                hBar.style.width = `${healthPct}%`;
                hBar.className   = `ov-kpi-bar-fill${healthPct < 70 ? " is-hot" : healthPct < 90 ? " is-warm" : ""}`;
            }

            // Anomalies count
            const anCount = ovLiveDevices.filter((d) => {
                if (d.status !== "ok" || d.error) return true;
                return Object.values(d.metrics || {}).some(m => m.quality !== "good");
            }).length;
            const anEl  = kpi("anomalies");
            if (anEl) anEl.textContent = String(anCount);
            const anSub = kpiSub("anomalies");
            if (anSub) anSub.textContent = anCount === 0 ? "All systems nominal" : `${anCount} device${anCount > 1 ? "s" : ""} affected`;
        };

        // ── Load live data → overlay on skeletons ──────────────────────────
        const loadLive = async () => {
            try {
                const r = await fetch("/api/insights/live");
                if (!r.ok) return;
                const d = await r.json();
                if (!d.ok) return;
                ovLiveDevices = d.devices || [];
                applyLiveToCards();
                updateKPIs();
            } catch (e) {
                console.warn("[Insights] loadLive failed:", e);
            }
        };

        // ── Tab switching ─────────────────────────────────────────────────
        insightsShell.querySelectorAll("[data-ov-tab]").forEach((btn) => {
            btn.addEventListener("click", () => {
                if (btn.disabled) return;
                insightsShell.querySelectorAll("[data-ov-tab]").forEach((b) => b.classList.remove("is-current"));
                insightsShell.querySelectorAll("[data-ov-panel]").forEach((p) => p.classList.add("ov-hidden"));
                btn.classList.add("is-current");
                const panel = insightsShell.querySelector(`[data-ov-panel="${btn.getAttribute("data-ov-tab")}"]`);
                panel?.classList.remove("ov-hidden");
                // Sensors tab: force chart render now that the panel is visible
                if (btn.getAttribute("data-ov-tab") === "sensors" && svRefreshFn) {
                    svRefreshFn(true);
                }
                // Events tab: force event + rules load on tab open
                if (btn.getAttribute("data-ov-tab") === "events" && evRefreshFn) {
                    evRefreshFn();
                }
            });
        });

        // ══════════════════════════════════════════════════════════════════════
        //  Sensors tab — per-device deep dive with chart
        // ══════════════════════════════════════════════════════════════════════
        const svPanel = insightsShell.querySelector('[data-ov-panel="sensors"]');
        if (svPanel) {
            // ── State (key selections persisted across page navigations) ─────────
            let svSelectedKey = localStorage.getItem("metacrust.sv.selectedKey") || "";
            let svWindow      = localStorage.getItem("metacrust.sv.window")      || "1h";
            let svChartData   = {};    // { metricName: { timestamps, avg, min, max, count } }
            let svLastFetchMs = 0;     // throttle chart re-fetches to 60 s
            let svLastTabKeys = "";    // detect configured set changes

            // ── DOM refs ───────────────────────────────────────────────────────
            const svDeviceTabs   = svPanel.querySelector("[data-sv-device-tabs]");
            const svDeviceHeader = svPanel.querySelector("[data-sv-device-header]");
            const svMetricList   = svPanel.querySelector("[data-sv-metric-list]");
            const svChart        = svPanel.querySelector("[data-sv-chart]");
            const svChartEmpty   = svPanel.querySelector("[data-sv-chart-empty]");
            const svLegend       = svPanel.querySelector("[data-sv-legend]");
            const svStats        = svPanel.querySelector("[data-sv-stats]");
            const svWindowBar    = svPanel.querySelector("[data-sv-window-bar]");
            const svExportBtn    = svPanel.querySelector("[data-sv-export-btn]");
            const svNoDevice     = svPanel.querySelector("[data-sv-no-device]");
            const svBody         = svPanel.querySelector("[data-sv-body]");

            // ── Chart colours (up to 4 overlaid metrics) ──────────────────────
            const SV_COLORS = ["#39d0c8", "#f0a64b", "#a78bfa", "#fb7185"];

            // ── Metric toggle persistence (localStorage) ───────────────────────
            const SV_LS_KEY = "metacrust.sv.toggles";
            const svToggles = (() => {
                try { return JSON.parse(localStorage.getItem(SV_LS_KEY) || "{}"); }
                catch { return {}; }
            })();
            const svSaveToggles = () => {
                try { localStorage.setItem(SV_LS_KEY, JSON.stringify(svToggles)); } catch {}
            };
            const svGetToggle = (dKey, mName) =>
                (svToggles[dKey] || {})[mName] !== false;   // default ON
            const svSetToggle = (dKey, mName, val) => {
                if (!svToggles[dKey]) svToggles[dKey] = {};
                svToggles[dKey][mName] = val;
                svSaveToggles();
            };

            // ── Device tabs ────────────────────────────────────────────────────
            const svBuildTabs = () => {
                if (!svDeviceTabs) return;
                const keys = ovConfigured.map((d) => `${d.source}:${d.device_id}`).join(",");
                if (keys === svLastTabKeys) return;
                svLastTabKeys = keys;

                if (ovConfigured.length === 0) {
                    svDeviceTabs.innerHTML = "";
                    svBody?.classList.add("ov-hidden");
                    svNoDevice?.classList.remove("ov-hidden");
                    return;
                }
                svBody?.classList.remove("ov-hidden");
                svNoDevice?.classList.add("ov-hidden");

                if (!svSelectedKey || !ovConfigured.some((d) => `${d.source}:${d.device_id}` === svSelectedKey)) {
                    svSelectedKey = `${ovConfigured[0].source}:${ovConfigured[0].device_id}`;
                }

                svDeviceTabs.innerHTML = ovConfigured.map((d) => {
                    const key  = `${d.source}:${d.device_id}`;
                    const curr = key === svSelectedKey;
                    return `<button class="sv-device-tab${curr ? " is-current" : ""}" data-sv-key="${key}">${d.name || d.device_id}</button>`;
                }).join("");

                svDeviceTabs.querySelectorAll("[data-sv-key]").forEach((btn) => {
                    btn.addEventListener("click", () => {
                        const newKey = btn.getAttribute("data-sv-key");
                        if (newKey === svSelectedKey) return;
                        svSelectedKey = newKey;
                        localStorage.setItem("metacrust.sv.selectedKey", svSelectedKey);
                        svDeviceTabs.querySelectorAll("[data-sv-key]").forEach((b) => b.classList.remove("is-current"));
                        btn.classList.add("is-current");
                        svChartData   = {};
                        svLastFetchMs = 0;
                        svStatsData       = {};
                        svStatsLastFetch  = 0;
                        svTrendsData      = [];
                        svTrendsLastFetch = 0;
                        svRenderMetricList();
                        svFetchAndRenderChart();
                    });
                });
            };

            // ── Metric list ────────────────────────────────────────────────────
            const svRenderMetricList = () => {
                if (!svMetricList || !svDeviceHeader) return;
                const device = ovConfigured.find((d) => `${d.source}:${d.device_id}` === svSelectedKey);
                if (!device) {
                    svDeviceHeader.innerHTML = "";
                    svMetricList.innerHTML   = "";
                    return;
                }
                const live     = ovLiveDevices.find((d) => `${d.source}:${d.device_id}` === svSelectedKey);
                const dStatus  = live?.status || "offline";
                const sc       = dStatus === "ok" ? "ov-status-live" : dStatus === "warning" ? "ov-status-warning" : dStatus === "error" ? "ov-status-error" : "ov-status-offline";
                const sl       = dStatus === "ok" ? "Live" : dStatus === "warning" ? "Warning" : dStatus === "error" ? "Error" : "Offline";
                const tp       = tpStr(device.transport);

                svDeviceHeader.innerHTML = `
                    <div>
                        <p class="sv-device-name">${device.name || device.device_id}</p>
                        ${tp ? `<p class="sv-device-transport">${tp}</p>` : ""}
                    </div>
                    <span class="ov-status-badge ${sc}">
                        <span class="ov-status-pulse"></span>
                        <span>${sl}</span>
                    </span>`;

                const metrics = device.expected_metrics || [];
                svMetricList.innerHTML = metrics.map((m) => {
                    const lm   = (live?.metrics || {})[m.name] || {};
                    const val  = lm.value !== undefined ? fmtVal(lm.value) : "--";
                    const unit = (m.unit || lm.unit || "").trim();
                    const q    = lm.quality || (live ? "good" : "none");
                    const on   = svGetToggle(svSelectedKey, m.name);
                    const lbl  = displayLabel(m.name);
                    return `
                        <div class="sv-metric-row" data-sv-row="${m.name}">
                            <span class="sv-quality-dot${q === "good" ? " is-good" : q === "stale" ? " is-stale" : q === "error" ? " is-error" : ""}"></span>
                            <span class="sv-metric-name" title="${lbl}">${lbl}</span>
                            <div class="sv-metric-reading">
                                <span class="sv-metric-val${q === "stale" ? " is-stale" : q === "error" ? " is-error" : ""}" data-sv-val="${m.name}">${val}</span>
                                <span class="sv-metric-unit">${unit}</span>
                            </div>
                            <span class="sv-quality-pill is-${q}">${q === "none" ? "—" : q}</span>
                            <button class="sv-toggle-btn${on ? " is-on" : ""}" data-sv-toggle="${m.name}">${on ? "ON" : "OFF"}</button>
                        </div>`;
                }).join("");

                svMetricList.querySelectorAll("[data-sv-toggle]").forEach((btn) => {
                    btn.addEventListener("click", () => {
                        const mName = btn.getAttribute("data-sv-toggle");
                        const curr  = svGetToggle(svSelectedKey, mName);
                        svSetToggle(svSelectedKey, mName, !curr);
                        btn.classList.toggle("is-on", !curr);
                        btn.textContent = !curr ? "ON" : "OFF";
                        svChartData   = {};
                        svLastFetchMs = 0;
                        svFetchAndRenderChart();
                    });
                });
            };

            // ── Live value updates (called every 3 s, no DOM rebuild) ──────────
            const svUpdateLiveValues = () => {
                if (!svMetricList) return;
                const live = ovLiveDevices.find((d) => `${d.source}:${d.device_id}` === svSelectedKey);

                // Also update the status badge in the device header
                if (svDeviceHeader && live) {
                    const badge  = svDeviceHeader.querySelector(".ov-status-badge");
                    const bText  = badge?.querySelector("span:last-child");
                    const dStatus = live.status || "ok";
                    if (badge) {
                        const sc = dStatus === "ok" ? "ov-status-live" : dStatus === "warning" ? "ov-status-warning" : "ov-status-error";
                        badge.className = `ov-status-badge ${sc}`;
                        if (bText) bText.textContent = dStatus === "ok" ? "Live" : dStatus === "warning" ? "Warning" : "Error";
                    }
                }

                if (!live) return;
                svMetricList.querySelectorAll("[data-sv-row]").forEach((row) => {
                    const mName  = row.getAttribute("data-sv-row");
                    const m      = (live.metrics || {})[mName];
                    if (!m) return;
                    const q      = m.quality || "good";
                    const valEl  = row.querySelector(`[data-sv-val="${mName}"]`);
                    const dotEl  = row.querySelector(".sv-quality-dot");
                    const pillEl = row.querySelector(".sv-quality-pill");
                    if (valEl)  { valEl.textContent = fmtVal(m.value); valEl.className = `sv-metric-val${q === "stale" ? " is-stale" : q === "error" ? " is-error" : ""}`; }
                    if (dotEl)  dotEl.className  = `sv-quality-dot${q === "good" ? " is-good" : q === "stale" ? " is-stale" : " is-error"}`;
                    if (pillEl) { pillEl.className = `sv-quality-pill is-${q}`; pillEl.textContent = q; }
                });
            };

            // ── History fetch ──────────────────────────────────────────────────
            const svFetchAndRenderChart = async () => {
                if (!svSelectedKey) return;
                const device = ovConfigured.find((d) => `${d.source}:${d.device_id}` === svSelectedKey);
                if (!device) return;

                const activeMetrics = (device.expected_metrics || [])
                    .filter((m) => svGetToggle(svSelectedKey, m.name))
                    .slice(0, 4)
                    .map((m) => m.name);

                if (activeMetrics.length === 0) {
                    svChartData = {};
                    svRenderChart();
                    svRenderStats();
                    return;
                }

                const [src, ...didParts] = svSelectedKey.split(":");
                const did    = didParts.join(":");
                const params = new URLSearchParams({
                    source:    src,
                    device_id: did,
                    metrics:   activeMetrics.join(","),
                    window:    svWindow,
                });

                try {
                    const r = await fetch(`/api/insights/history?${params}`);
                    if (!r.ok) return;
                    const d = await r.json();
                    if (!d.ok) return;
                    svChartData   = d.metrics || {};
                    svLastFetchMs = Date.now();
                } catch (e) {
                    console.warn("[Sensors] history fetch failed:", e);
                    return;
                }
                svRenderChart();
                svRenderStats();
            };

            // ═══════════════════════════════════════════════════════════════════
            //  Core chart drawing — shared by small chart + expanded modal
            // ═══════════════════════════════════════════════════════════════════
            const svDrawIntoSvg = (svgEl, data, visMetrics, zStart = 0, zEnd = 1) => {
                const active = visMetrics.filter((k) => (data[k]?.timestamps?.length || 0) > 1);
                if (!active.length) return null;

                // Stable color map: color is determined by position in the FULL data keyset,
                // so colors never shift when individual metrics are toggled off.
                const allDataKeys = Object.keys(data);
                const colorMap = {};
                allDataKeys.forEach((k, i) => { colorMap[k] = SV_COLORS[i % SV_COLORS.length]; });

                const W = svgEl.clientWidth  || 600;
                const H = svgEl.clientHeight || 260;
                const padL = 48, padR = 14, padT = 14, padB = 34;
                const cW = W - padL - padR;
                const cH = H - padT - padB;

                // Full time range, then apply zoom window
                const allTs     = active.flatMap((k) => data[k].timestamps);
                const gMin = Math.min(...allTs), gMax = Math.max(...allTs);
                const gRng      = gMax - gMin || 1;
                const visMin    = gMin + zStart * gRng;
                const visMax    = gMin + zEnd   * gRng;
                const visRng    = visMax - visMin || 1;
                const inView    = (ts) => ts >= visMin - visRng * 0.01 && ts <= visMax + visRng * 0.01;
                const toX       = (ts) => padL + ((ts - visMin) / visRng) * cW;

                // Per-metric Y scale (each metric normalised to its visible range)
                const mScale = {};
                for (const k of active) {
                    const vals = data[k].timestamps.map((ts, i) => inView(ts) ? data[k].avg[i] : null).filter((v) => v !== null);
                    const mn = vals.length ? Math.min(...vals) : 0;
                    const mx = vals.length ? Math.max(...vals) : 1;
                    mScale[k] = { min: mn, rng: mx - mn || 1, max: mx };
                }
                const toY = (k, v) => padT + cH - ((v - mScale[k].min) / mScale[k].rng) * cH;

                const parts = [];

                // Grid lines
                for (let i = 0; i <= 4; i++) {
                    const y = padT + (i / 4) * cH;
                    parts.push(`<line x1="${padL}" y1="${y.toFixed(1)}" x2="${(W - padR).toFixed(1)}" y2="${y.toFixed(1)}" stroke="rgba(255,255,255,0.04)" stroke-width="1"/>`);
                }

                // Y axis labels (for first metric only — shows normalised %)
                for (let i = 0; i <= 4; i++) {
                    const y   = padT + (i / 4) * cH;
                    const pct = 100 - i * 25;
                    parts.push(`<text x="${padL - 4}" y="${(y + 3.5).toFixed(1)}" fill="rgba(157,179,187,0.4)" font-size="9" text-anchor="end" font-family="monospace">${pct}%</text>`);
                }

                // X axis time labels
                const lblCount = Math.min(6, Math.max(3, Math.floor(W / 110)));
                for (let i = 0; i <= lblCount; i++) {
                    const ts  = visMin + (i / lblCount) * visRng;
                    const x   = padL + (i / lblCount) * cW;
                    const dt  = new Date(ts);
                    const lbl = dt.getHours().toString().padStart(2, "0") + ":" + dt.getMinutes().toString().padStart(2, "0");
                    parts.push(`<text x="${x.toFixed(1)}" y="${H - 9}" fill="rgba(157,179,187,0.5)" font-size="10" text-anchor="middle" font-family="monospace">${lbl}</text>`);
                }

                // Crosshair (hidden by default)
                parts.push(`<line class="sv-xhair" x1="-9" y1="${padT}" x2="-9" y2="${padT + cH}" stroke="rgba(255,255,255,0.22)" stroke-width="1" stroke-dasharray="3,2" pointer-events="none"/>`);

                // Per-metric area + line
                active.forEach((k) => {
                    const color  = colorMap[k];
                    const series = data[k];
                    const pts    = series.timestamps
                        .map((ts, i) => (series.avg[i] !== null && inView(ts)) ? { x: toX(ts), y: toY(k, series.avg[i]), ts, v: series.avg[i] } : null)
                        .filter(Boolean);
                    if (pts.length < 2) return;
                    const lineD = pts.map((p, i) => `${i === 0 ? "M" : "L"}${p.x.toFixed(1)},${p.y.toFixed(1)}`).join(" ");
                    const areaD = `${lineD} L${pts[pts.length-1].x.toFixed(1)},${(padT+cH).toFixed(1)} L${pts[0].x.toFixed(1)},${(padT+cH).toFixed(1)}Z`;
                    parts.push(`<path d="${areaD}" fill="${color}" fill-opacity="0.07" stroke="none"/>`);
                    parts.push(`<path d="${lineD}" fill="none" stroke="${color}" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" opacity="0.85"/>`);
                });

                svgEl.setAttribute("viewBox", `0 0 ${W} ${H}`);
                svgEl.innerHTML = parts.join("");

                // Return state object for hit-testing (tooltip)
                return {
                    active, mScale, colorMap, visMin, visMax, visRng, padL, cW, gMin, gMax, gRng,
                    hitTest(xFrac) {
                        const ts = visMin + xFrac * visRng;
                        const result = {};
                        for (const k of active) {
                            const s = data[k];
                            let best = null, bestD = Infinity;
                            for (let i = 0; i < s.timestamps.length; i++) {
                                if (!inView(s.timestamps[i])) continue;
                                const d = Math.abs(s.timestamps[i] - ts);
                                if (d < bestD) { bestD = d; best = { ts: s.timestamps[i], v: s.avg[i], min: s.min[i], max: s.max[i] }; }
                            }
                            if (best) result[k] = best;
                        }
                        return result;
                    },
                };
            };

            // ── Hover tooltip handler ─────────────────────────────────────────
            const svAttachHover = (wrapEl, svgEl, ttEl, getState) => {
                if (!wrapEl || !svgEl || !ttEl) return;
                const onMove = (e) => {
                    const st = getState();
                    if (!st) { ttEl.style.display = "none"; return; }
                    const svgRect = svgEl.getBoundingClientRect();
                    const rawX    = e.clientX - svgRect.left;
                    const xFrac   = Math.max(0, Math.min(1, (rawX - 48) / (svgRect.width - 62)));
                    const hit     = st.hitTest(xFrac);
                    if (!Object.keys(hit).length) { ttEl.style.display = "none"; return; }

                    const xhair = svgEl.querySelector(".sv-xhair");
                    if (xhair) { const cx = rawX.toFixed(1); xhair.setAttribute("x1", cx); xhair.setAttribute("x2", cx); }

                    const firstTs = Object.values(hit)[0]?.ts;
                    const dt = firstTs ? new Date(firstTs) : null;
                    const hdr = dt
                        ? dt.toLocaleDateString([], { month: "short", day: "numeric" }) + "  " + dt.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })
                        : "";
                    const rows = st.active.map((k) => {
                        const h = hit[k]; if (!h) return "";
                        const color = st.colorMap?.[k] || SV_COLORS[0];
                        const pct = Math.round(((h.v - st.mScale[k].min) / st.mScale[k].rng) * 100);
                        return `<div class="sv-tt-row"><span class="sv-tt-dot" style="background:${color}"></span><span class="sv-tt-label">${displayLabel(k)}</span><span class="sv-tt-val">${fmtVal(h.v)}</span><span class="sv-tt-pct">${pct}%</span></div>`;
                    }).join("");
                    ttEl.innerHTML = `<div class="sv-tt-hdr">${hdr}</div>${rows}`;
                    ttEl.style.display = "block";

                    const wrapRect = wrapEl.getBoundingClientRect();
                    const lx = e.clientX - wrapRect.left + 14;
                    const flip = lx + 175 > wrapRect.width;
                    ttEl.style.left = `${flip ? lx - 190 : lx}px`;
                    ttEl.style.top  = `${Math.max(0, e.clientY - wrapRect.top - 20)}px`;
                };
                wrapEl.addEventListener("mousemove", onMove);
                wrapEl.addEventListener("mouseleave", () => {
                    ttEl.style.display = "none";
                    const xhair = svgEl.querySelector(".sv-xhair");
                    if (xhair) { xhair.setAttribute("x1", "-9"); xhair.setAttribute("x2", "-9"); }
                });
            };

            // ── Small chart render ─────────────────────────────────────────────
            // Inject tooltip overlay + expand button once
            const svChartWrap = svPanel.querySelector(".sv-chart-wrap");
            let svSmallTt = null, svExpandBtn = null;
            if (svChartWrap) {
                svSmallTt = document.createElement("div");
                svSmallTt.className = "sv-tooltip";
                svSmallTt.style.display = "none";
                svChartWrap.appendChild(svSmallTt);

                svExpandBtn = document.createElement("button");
                svExpandBtn.className = "sv-expand-btn";
                svExpandBtn.title = "Expand / fullscreen";
                svExpandBtn.innerHTML = `<svg width="14" height="14" viewBox="0 0 14 14" fill="none"><path d="M1 5V1h4M9 1h4v4M13 9v4H9M5 13H1V9" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/></svg>`;
                svChartWrap.appendChild(svExpandBtn);
            }

            let svSmallChartState = null;
            svAttachHover(svChartWrap, svChart, svSmallTt, () => svSmallChartState);

            const svRenderChart = () => {
                if (!svChart) return;
                const active = Object.keys(svChartData).filter((k) => (svChartData[k]?.timestamps?.length || 0) > 1);
                if (active.length === 0) {
                    svChart.innerHTML = "";
                    svSmallChartState = null;
                    svChartEmpty?.classList.remove("ov-hidden");
                    if (svLegend) svLegend.innerHTML = "";
                    return;
                }
                svChartEmpty?.classList.add("ov-hidden");
                svSmallChartState = svDrawIntoSvg(svChart, svChartData, active, 0, 1);
                if (svLegend && svSmallChartState) {
                    const cm = svSmallChartState.colorMap;
                    svLegend.innerHTML = active.map((k) =>
                        `<span class="sv-legend-item"><span class="sv-legend-dot" style="background:${cm[k]}"></span>${displayLabel(k)}</span>`
                    ).join("");
                }
            };

            // ── Expanded modal ─────────────────────────────────────────────────
            let svModal        = null;
            let svModalVisible = new Set();
            let svZoomStart    = 0;
            let svZoomEnd      = 1;
            let svModalState   = null;

            const svBuildModal = () => {
                if (svModal) return;
                svModal = document.createElement("div");
                svModal.className = "sv-chart-modal";
                svModal.innerHTML = `
                    <div class="sv-modal-inner">
                        <header class="sv-modal-head">
                            <span class="sv-modal-device-name" data-sv-modal-dev></span>
                            <span class="sv-modal-window-lbl" data-sv-modal-win-lbl></span>
                            <div class="sv-modal-line-btns" data-sv-modal-line-btns></div>
                            <button class="sv-modal-close-btn" data-sv-modal-close>✕</button>
                        </header>
                        <div class="sv-modal-chart-area" data-sv-modal-area>
                            <svg class="sv-modal-svg" data-sv-modal-svg preserveAspectRatio="none"></svg>
                            <div class="sv-modal-tt" data-sv-modal-tt></div>
                        </div>
                        <footer class="sv-modal-foot">
                            <div class="sv-zoom-ctrls">
                                <button class="sv-zoom-btn" data-sv-zi>＋</button>
                                <button class="sv-zoom-btn" data-sv-zo>－</button>
                                <button class="sv-zoom-reset-btn" data-sv-zr>Reset zoom</button>
                            </div>
                            <div class="sv-minimap" data-sv-minimap>
                                <div class="sv-minimap-full" data-sv-mini-full></div>
                                <div class="sv-minimap-win"  data-sv-mini-win></div>
                            </div>
                        </footer>
                    </div>`;
                document.body.appendChild(svModal);

                const modalSvg  = svModal.querySelector("[data-sv-modal-svg]");
                const modalArea = svModal.querySelector("[data-sv-modal-area]");
                const modalTt   = svModal.querySelector("[data-sv-modal-tt]");
                const lineBtns  = svModal.querySelector("[data-sv-modal-line-btns]");
                const miniWin   = svModal.querySelector("[data-sv-mini-win]");
                const winLbl    = svModal.querySelector("[data-sv-modal-win-lbl]");

                const redraw = () => {
                    const vis = [...svModalVisible];
                    svModalState = svDrawIntoSvg(modalSvg, svChartData, vis, svZoomStart, svZoomEnd);
                    if (miniWin) {
                        miniWin.style.left  = `${svZoomStart * 100}%`;
                        miniWin.style.width = `${(svZoomEnd - svZoomStart) * 100}%`;
                    }
                    if (winLbl && svModalState) {
                        const fmtTs = (ms) => new Date(ms).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
                        const vMin  = svModalState.gMin + svZoomStart * svModalState.gRng;
                        const vMax  = svModalState.gMin + svZoomEnd   * svModalState.gRng;
                        winLbl.textContent = `${fmtTs(vMin)} – ${fmtTs(vMax)}`;
                    }
                };

                const buildLineBtns = () => {
                    if (!lineBtns) return;
                    const active = Object.keys(svChartData).filter((k) => (svChartData[k]?.timestamps?.length || 0) > 1);
                    svModalVisible = new Set(active);
                    // Stable color map so buttons always match the chart lines
                    const cm = {};
                    Object.keys(svChartData).forEach((k, i) => { cm[k] = SV_COLORS[i % SV_COLORS.length]; });
                    lineBtns.innerHTML = active.map((k) => {
                        const c = cm[k];
                        return `<button class="sv-modal-line-btn is-on" data-mline="${k}"><span class="sv-modal-line-dot" style="background:${c}"></span>${displayLabel(k)}</button>`;
                    }).join("");
                    lineBtns.querySelectorAll("[data-mline]").forEach((btn) => {
                        btn.addEventListener("click", () => {
                            const k = btn.getAttribute("data-mline");
                            if (svModalVisible.has(k)) {
                                if (svModalVisible.size > 1) { svModalVisible.delete(k); btn.classList.remove("is-on"); }
                            } else { svModalVisible.add(k); btn.classList.add("is-on"); }
                            redraw();
                        });
                    });
                };
                svModal._buildLineBtns = buildLineBtns;
                svModal._redraw        = redraw;

                svAttachHover(modalArea, modalSvg, modalTt, () => svModalState);

                // Mouse wheel zoom
                modalArea.addEventListener("wheel", (e) => {
                    e.preventDefault();
                    const rng = svZoomEnd - svZoomStart;
                    const factor = e.deltaY > 0 ? 1.3 : 0.77;
                    const newRng = Math.max(0.05, Math.min(1, rng * factor));
                    const r      = modalArea.getBoundingClientRect();
                    const frac   = (e.clientX - r.left) / r.width;
                    const center = svZoomStart + frac * rng;
                    svZoomStart = Math.max(0, center - newRng * frac);
                    svZoomEnd   = Math.min(1, svZoomStart + newRng);
                    if (svZoomEnd > 1) { svZoomStart = Math.max(0, 1 - newRng); svZoomEnd = 1; }
                    redraw();
                }, { passive: false });

                // Drag to pan
                let dragX = null, dZS = 0, dZE = 1;
                modalArea.addEventListener("mousedown", (e) => {
                    if (e.button !== 0) return;
                    dragX = e.clientX; dZS = svZoomStart; dZE = svZoomEnd;
                    modalArea.style.cursor = "grabbing";
                });
                document.addEventListener("mousemove", (e) => {
                    if (dragX === null) return;
                    const r   = modalArea.getBoundingClientRect();
                    const dx  = (dragX - e.clientX) / r.width;
                    const rng = dZE - dZS;
                    let ns = dZS + dx * rng * 3, ne = dZE + dx * rng * 3;
                    if (ns < 0) { ne += -ns; ns = 0; }
                    if (ne > 1) { ns -= ne - 1; ne = 1; }
                    svZoomStart = Math.max(0, ns); svZoomEnd = Math.min(1, ne);
                    redraw();
                });
                document.addEventListener("mouseup", () => { dragX = null; if (modalArea) modalArea.style.cursor = ""; });

                // Minimap click
                const miniFull = svModal.querySelector("[data-sv-mini-full]");
                if (miniFull) {
                    miniFull.parentElement.addEventListener("click", (e) => {
                        const r    = miniFull.parentElement.getBoundingClientRect();
                        const frac = (e.clientX - r.left) / r.width;
                        const rng  = svZoomEnd - svZoomStart;
                        svZoomStart = Math.max(0, frac - rng / 2);
                        svZoomEnd   = Math.min(1, svZoomStart + rng);
                        redraw();
                    });
                }

                // Zoom buttons
                svModal.querySelector("[data-sv-zi]")?.addEventListener("click", () => { const c=(svZoomStart+svZoomEnd)/2, r=(svZoomEnd-svZoomStart)*0.65; svZoomStart=Math.max(0,c-r/2); svZoomEnd=Math.min(1,c+r/2); redraw(); });
                svModal.querySelector("[data-sv-zo]")?.addEventListener("click", () => { const c=(svZoomStart+svZoomEnd)/2, r=Math.min(1,(svZoomEnd-svZoomStart)*1.5); svZoomStart=Math.max(0,c-r/2); svZoomEnd=Math.min(1,c+r/2); redraw(); });
                svModal.querySelector("[data-sv-zr]")?.addEventListener("click", () => { svZoomStart=0; svZoomEnd=1; redraw(); });

                // Close
                svModal.querySelector("[data-sv-modal-close]")?.addEventListener("click", svCloseModal);
                svModal.addEventListener("click", (e) => { if (e.target === svModal) svCloseModal(); });
                document.addEventListener("keydown", (e) => { if (e.key === "Escape" && svModal?.classList.contains("is-open")) svCloseModal(); });
            };

            const svOpenModal = () => {
                svBuildModal();
                const device = ovConfigured.find((d) => `${d.source}:${d.device_id}` === svSelectedKey);
                const devEl  = svModal.querySelector("[data-sv-modal-dev]");
                if (devEl) devEl.textContent = `${device?.name || svSelectedKey}  ·  ${svWindow}`;
                svZoomStart = 0; svZoomEnd = 1;
                svModal._buildLineBtns?.();
                svModal._redraw?.();
                svModal.classList.add("is-open");
                document.body.style.overflow = "hidden";
            };
            const svCloseModal = () => {
                svModal?.classList.remove("is-open");
                document.body.style.overflow = "";
            };

            if (svExpandBtn) {
                svExpandBtn.addEventListener("click", () => {
                    if (Object.keys(svChartData).length > 0) svOpenModal();
                });
            }

            // ── Stats strip ────────────────────────────────────────────────────
            const svRenderStats = () => {
                if (!svStats) return;
                const active = Object.keys(svChartData).filter((k) => (svChartData[k]?.timestamps?.length || 0) > 0);
                if (active.length === 0) { svStats.innerHTML = ""; return; }

                // Stable colors: same map used by svDrawIntoSvg
                const cm = {};
                Object.keys(svChartData).forEach((k, i) => { cm[k] = SV_COLORS[i % SV_COLORS.length]; });

                svStats.innerHTML = active.map((k) => {
                    const color  = cm[k];
                    const series = svChartData[k];
                    const avgs   = series.avg.filter((v) => v !== null);
                    const mins   = series.min.filter((v) => v !== null);
                    const maxs   = series.max.filter((v) => v !== null);
                    const total  = series.count.reduce((a, b) => a + (b || 0), 0);
                    const avg    = avgs.length ? fmtVal(avgs.reduce((a, b) => a + b, 0) / avgs.length) : "--";
                    const min    = mins.length ? fmtVal(Math.min(...mins)) : "--";
                    const max    = maxs.length ? fmtVal(Math.max(...maxs)) : "--";
                    return `
                        <div class="sv-stat-card">
                            <p class="sv-stat-name">
                                <span class="sv-stat-color" style="background:${color}"></span>
                                ${displayLabel(k)}
                            </p>
                            <div class="sv-stat-grid">
                                <div class="sv-stat-item"><span class="sv-stat-label">Avg</span><span class="sv-stat-value">${avg}</span></div>
                                <div class="sv-stat-item"><span class="sv-stat-label">Min</span><span class="sv-stat-value">${min}</span></div>
                                <div class="sv-stat-item"><span class="sv-stat-label">Max</span><span class="sv-stat-value">${max}</span></div>
                                <div class="sv-stat-item"><span class="sv-stat-label">Samples</span><span class="sv-stat-value">${total.toLocaleString()}</span></div>
                            </div>
                        </div>`;
                }).join("");
            };

            // ── Window bar ─────────────────────────────────────────────────────
            if (svWindowBar) {
                svWindowBar.querySelectorAll("[data-sv-win]").forEach((btn) => {
                    if (btn.getAttribute("data-sv-win") === svWindow) btn.classList.add("is-current");
                    btn.addEventListener("click", () => {
                        svWindow = btn.getAttribute("data-sv-win");
                        localStorage.setItem("metacrust.sv.window", svWindow);
                        svWindowBar.querySelectorAll("[data-sv-win]").forEach((b) => b.classList.remove("is-current"));
                        btn.classList.add("is-current");
                        svChartData   = {};
                        svLastFetchMs = 0;
                        svFetchAndRenderChart();
                    });
                });
            }

            // ── Export CSV ─────────────────────────────────────────────────────
            if (svExportBtn) {
                svExportBtn.addEventListener("click", () => {
                    if (!svSelectedKey) return;

                    const [src, ...didParts] = svSelectedKey.split(":");
                    const did    = didParts.join(":");
                    const device = ovConfigured.find((d) => d.source === src && d.device_id === did);

                    // Export the ON-toggled metrics (or all if none toggled)
                    const toggledMetrics = (device?.expected_metrics || [])
                        .filter((m) => svGetToggle(svSelectedKey, m.name))
                        .map((m) => m.name);

                    const params = new URLSearchParams({
                        source:    src,
                        device_id: did,
                        metrics:   toggledMetrics.length ? toggledMetrics.join(",") : "all",
                        window:    svWindow,
                        name:      device?.name || did,
                    });

                    // Trigger file download — browser handles the rest
                    const a = document.createElement("a");
                    a.href = `/api/insights/export/csv?${params}`;
                    a.download = "";
                    document.body.appendChild(a);
                    a.click();
                    document.body.removeChild(a);
                });
            }

            // ══════════════════════════════════════════════════════════════════════
            //  Analytics panel — Rolling Stats (Tier 2) + Trends (Tier 3)
            // ══════════════════════════════════════════════════════════════════════
            const svAnalyticsPanel = svPanel.querySelector(".sv-analytics-panel");
            const svStatsTable     = svPanel.querySelector("[data-sv-stats-table]");
            const svStatsEmpty     = svPanel.querySelector("[data-sv-stats-empty]");
            const svTrendsList     = svPanel.querySelector("[data-sv-trends-list]");
            const svTrendsEmpty    = svPanel.querySelector("[data-sv-trends-empty]");

            // ── Analytics panel tab switching ──────────────────────────────────
            if (svAnalyticsPanel) {
                svAnalyticsPanel.querySelectorAll("[data-sv-atab]").forEach((btn) => {
                    btn.addEventListener("click", () => {
                        const tab = btn.getAttribute("data-sv-atab");
                        svAnalyticsPanel.querySelectorAll("[data-sv-atab]").forEach((b) => b.classList.remove("is-current"));
                        svAnalyticsPanel.querySelectorAll("[data-sv-abody]").forEach((p) => p.classList.add("ov-hidden"));
                        btn.classList.add("is-current");
                        svAnalyticsPanel.querySelector(`[data-sv-abody="${tab}"]`)?.classList.remove("ov-hidden");
                    });
                });
            }

            // ── Rolling stats ──────────────────────────────────────────────────
            let svStatsWindow    = localStorage.getItem("metacrust.sv.statsWindow") || "5min";
            let svStatsData      = {};     // { window → { metric → stats } }
            let svStatsLastFetch = 0;

            const svFmtStat = (v) => v !== null && v !== undefined ? fmtVal(v) : "--";

            const svRenderStatsTable = () => {
                if (!svStatsTable) return;
                const windowData = svStatsData[svStatsWindow] || {};
                const metrics    = Object.keys(windowData);
                if (metrics.length === 0) {
                    svStatsTable.innerHTML = "";
                    svStatsEmpty?.classList.remove("ov-hidden");
                    return;
                }
                svStatsEmpty?.classList.add("ov-hidden");

                // Show freshness note using computed_at from first metric
                const firstComputedAt = windowData[metrics[0]]?.computed_at;
                const freshnessNote   = firstComputedAt
                    ? ` · updated ${svTrendAge(firstComputedAt)}`
                    : "";

                svStatsTable.innerHTML = `
                    <p class="sv-stats-freshness">Stats for window: <strong>${svStatsWindow}</strong>${freshnessNote} · Health = good readings / total</p>
                    <table class="sv-stats-table">
                        <thead>
                            <tr>
                                <th>Metric</th>
                                <th>Avg</th>
                                <th>Min</th>
                                <th>Max</th>
                                <th>±Std Dev</th>
                                <th>Samples</th>
                                <th>Health</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${metrics.map((m) => {
                                const s  = windowData[m];
                                const hp = s.health_pct;
                                const hc = hp === null ? "" : hp >= 95 ? "is-good" : hp >= 80 ? "is-warn" : "is-crit";
                                const hv = hp !== null ? `${hp}%` : "--";
                                return `<tr>
                                    <td>${displayLabel(m)}</td>
                                    <td>${svFmtStat(s.avg)}</td>
                                    <td>${svFmtStat(s.min)}</td>
                                    <td>${svFmtStat(s.max)}</td>
                                    <td>${s.stddev !== null ? `±${svFmtStat(s.stddev)}` : "--"}</td>
                                    <td>${(s.sample_count || 0).toLocaleString()}</td>
                                    <td class="sv-stats-health-cell ${hc}">${hv}</td>
                                </tr>`;
                            }).join("")}
                        </tbody>
                    </table>`;
            };

            const svFetchStats = async () => {
                if (!svSelectedKey) return;
                const [src, ...rest] = svSelectedKey.split(":");
                const did = rest.join(":");
                try {
                    const r = await fetch(`/api/insights/stats?source=${encodeURIComponent(src)}&device_id=${encodeURIComponent(did)}`);
                    if (!r.ok) return;
                    const d = await r.json();
                    if (d.ok) { svStatsData = d.stats || {}; svStatsLastFetch = Date.now(); }
                } catch (e) { console.warn("[Sensors] stats fetch failed:", e); }
                svRenderStatsTable();
            };

            // Stats window bar
            if (svPanel) {
                svPanel.querySelectorAll("[data-sv-swin]").forEach((btn) => {
                    if (btn.getAttribute("data-sv-swin") === svStatsWindow) btn.classList.add("is-current");
                    btn.addEventListener("click", () => {
                        svStatsWindow = btn.getAttribute("data-sv-swin");
                        localStorage.setItem("metacrust.sv.statsWindow", svStatsWindow);
                        svPanel.querySelectorAll("[data-sv-swin]").forEach((b) => b.classList.remove("is-current"));
                        btn.classList.add("is-current");
                        svRenderStatsTable();
                    });
                });
            }

            // ── Trends ─────────────────────────────────────────────────────────
            const SV_SENS_PCT = { low: 10, medium: 3, high: 1 };
            let svSensitivity     = localStorage.getItem("metacrust.sv.sensitivity") || "medium";
            let svTrendsData      = [];
            let svTrendsLastFetch = 0;

            // Set saved sensitivity on load
            if (svPanel) {
                svPanel.querySelectorAll("[data-sv-sens]").forEach((btn) => {
                    if (btn.getAttribute("data-sv-sens") === svSensitivity) btn.classList.add("is-current");
                    else btn.classList.remove("is-current");
                    btn.addEventListener("click", () => {
                        svSensitivity = btn.getAttribute("data-sv-sens");
                        localStorage.setItem("metacrust.sv.sensitivity", svSensitivity);
                        svPanel.querySelectorAll("[data-sv-sens]").forEach((b) => b.classList.remove("is-current"));
                        btn.classList.add("is-current");
                        svRenderTrends();
                    });
                });
            }

            const svClassifySlope = (slope, currentValue) => {
                if (!currentValue || currentValue === 0) return slope > 0.001 ? "rising" : slope < -0.001 ? "falling" : "stable";
                const pctPerMin = Math.abs(slope / currentValue) * 100;
                const threshold = SV_SENS_PCT[svSensitivity] ?? 3;
                if (pctPerMin < threshold) return "stable";
                return slope > 0 ? "rising" : "falling";
            };

            const svTrendAge = (computedAtMs) => {
                if (!computedAtMs) return "";
                const ageSec = Math.round((Date.now() - computedAtMs) / 1000);
                if (ageSec < 60)  return `${ageSec}s ago`;
                if (ageSec < 3600) return `${Math.round(ageSec / 60)}m ago`;
                return `${Math.round(ageSec / 3600)}h ago`;
            };

            const svRenderTrends = () => {
                if (!svTrendsList) return;
                if (svTrendsData.length === 0) {
                    svTrendsList.innerHTML = "";
                    if (svTrendsEmpty) {
                        svTrendsEmpty.innerHTML = `
                            <p style="margin:0 0 0.35rem;font-weight:600;color:var(--muted)">No trend data yet</p>
                            <p style="margin:0;font-size:0.8rem;color:rgba(157,179,187,0.5)">
                                Trend analysis needs ~5 minutes of sensor readings.<br>
                                Check back shortly — data is collected automatically.
                            </p>`;
                        svTrendsEmpty.classList.remove("ov-hidden");
                    }
                    return;
                }
                svTrendsEmpty?.classList.add("ov-hidden");

                // Get live values for client-side sensitivity re-classification
                const liveDevice = ovLiveDevices.find((d) => `${d.source}:${d.device_id}` === svSelectedKey);
                const liveVals   = {};
                for (const [k, m] of Object.entries(liveDevice?.metrics || {})) {
                    if (m.value !== null && m.value !== undefined) liveVals[k] = m.value;
                }

                const DIRS  = { rising: "↑", falling: "↓", stable: "→" };
                const TONES = { rising: "is-rising", falling: "is-falling", stable: "is-stable" };

                svTrendsList.innerHTML = svTrendsData.map((t) => {
                    const slope    = t.slope ?? 0;
                    const currVal  = liveVals[t.metric_name];
                    const dir      = svClassifySlope(slope, currVal ?? 0);
                    const arrow    = DIRS[dir]  || "→";
                    const tone     = TONES[dir] || "is-stable";
                    const slopeAbs = Math.abs(slope);

                    // Slope description: direction word + rate
                    let slopeDesc;
                    if (slopeAbs < 0.0001) {
                        slopeDesc = "stable";
                    } else {
                        const rateStr = slopeAbs < 0.01
                            ? `${fmtVal(slope)}/min`
                            : `${slope >= 0 ? "+" : ""}${fmtVal(slope)}/min`;
                        slopeDesc = dir === "stable" ? `~${rateStr}` : rateStr;
                    }

                    // Current live value chip
                    const liveChip = currVal !== undefined
                        ? `<span class="sv-trend-live">${fmtVal(currVal)}</span>`
                        : "";

                    // Sample count + age
                    const samples  = t.n_samples ? `${t.n_samples} pts` : "";
                    const age      = svTrendAge(t.computed_at);
                    const metaStr  = [samples, age].filter(Boolean).join(" · ");

                    // Time-to-threshold
                    let tttHtml = "";
                    const ttt   = t.ttt_minutes;
                    const rule  = t.ttt_rule;
                    if (ttt !== null && ttt !== undefined && dir !== "stable") {
                        const sev  = (rule?.severity || "warning").toLowerCase();
                        const tCls = sev === "critical" ? "is-crit" : "is-warn";
                        const mins = ttt < 1 ? "<1" : ttt < 60 ? `${Math.round(ttt)}` : `${(ttt / 60).toFixed(1)}h`;
                        const unit = ttt >= 60 ? "" : " min";
                        tttHtml = `<span class="sv-trend-ttt ${tCls}" title="${sev.toUpperCase()} threshold: ${rule?.threshold}">⚡ ~${mins}${unit} to alert</span>`;
                    }

                    return `
                        <div class="sv-trend-row ${tone}">
                            <div class="sv-trend-top">
                                <span class="sv-trend-direction ${tone}">${arrow}</span>
                                <span class="sv-trend-metric">${displayLabel(t.metric_name)}</span>
                                ${liveChip}
                            </div>
                            <div class="sv-trend-bottom">
                                <span class="sv-trend-slope">${slopeDesc}</span>
                                ${metaStr ? `<span class="sv-trend-meta">${metaStr}</span>` : ""}
                                ${tttHtml}
                            </div>
                        </div>`;
                }).join("");
            };

            const svFetchTrends = async () => {
                if (!svSelectedKey) return;
                const [src, ...rest] = svSelectedKey.split(":");
                const did = rest.join(":");
                try {
                    const r = await fetch(`/api/insights/trends?source=${encodeURIComponent(src)}&device_id=${encodeURIComponent(did)}`);
                    if (!r.ok) return;
                    const d = await r.json();
                    if (d.ok) { svTrendsData = d.trends || []; svTrendsLastFetch = Date.now(); }
                } catch (e) { console.warn("[Sensors] trends fetch failed:", e); }
                svRenderTrends();
            };

            // ── Entry point (called from main refresh loop) ────────────────────
            const svRefresh = (force = false) => {
                const tabBtn   = insightsShell.querySelector('[data-ov-tab="sensors"]');
                const isActive = tabBtn?.classList.contains("is-current");

                svBuildTabs();   // no-op if device set unchanged

                if (!isActive) return;

                // On first activation, build the metric list for the selected device
                if (!svMetricList?.children.length) svRenderMetricList();

                svUpdateLiveValues();

                // Chart: at most every 60 s
                if (force || !svLastFetchMs || (Date.now() - svLastFetchMs) > 60_000) {
                    svFetchAndRenderChart();
                }

                // Rolling stats: at most every 30 s
                if (force || !svStatsLastFetch || (Date.now() - svStatsLastFetch) > 30_000) {
                    svFetchStats();
                }

                // Trends: every 10 s (slopes change quickly)
                if (force || !svTrendsLastFetch || (Date.now() - svTrendsLastFetch) > 10_000) {
                    svFetchTrends();
                } else {
                    // Re-render with current sensitivity even without new data
                    svRenderTrends();
                }
            };

            svRefreshFn = svRefresh;   // expose to outer refresh() loop
        }

        // ── Bootstrap ─────────────────────────────────────────────────────
        // Rebuild cards only when the configured device set changes
        let ovConfiguredKeys = "";

        const refresh = async () => {
            // ── Fetch configured devices ──────────────────────────────────
            let configured = [];
            try {
                const r = await fetch("/api/insights/configured");
                if (r.ok) {
                    const d = await r.json();
                    if (d.ok) configured = d.devices || [];
                }
            } catch (e) {
                console.error("[Insights] configured fetch failed:", e);
            }

            ovConfigured      = configured;
            ovConfiguredCount = configured.length;

            if (configured.length === 0) {
                ovNoSensors?.classList.remove("ov-hidden");
                if (ovDeviceGrid) ovDeviceGrid.innerHTML = "";
            } else {
                ovNoSensors?.classList.add("ov-hidden");

                const newKeys = configured.map((x) => `${x.source}:${x.device_id}`).join(",");
                if (newKeys !== ovConfiguredKeys && ovDeviceGrid) {
                    // Build each card individually so one bad device can't block the rest
                    const htmlParts = [];
                    for (const device of configured) {
                        try {
                            htmlParts.push(buildCard(device));
                        } catch (e) {
                            console.error("[Insights] buildCard failed for", device.name, e);
                            htmlParts.push(`
                                <article class="ov-card is-error">
                                    <header class="ov-card-head">
                                        <div><p class="ov-card-name">${device.name || device.device_id}</p></div>
                                        <span class="ov-status-badge ov-status-error"><span class="ov-status-pulse"></span><span>Error</span></span>
                                    </header>
                                    <div style="padding:1rem;color:var(--muted);font-size:.84rem">Card render error: ${e.message}</div>
                                </article>`);
                        }
                    }
                    ovDeviceGrid.innerHTML = htmlParts.join("");
                    ovConfiguredKeys = newKeys;
                }
            }

            // ── Overlay live Redis data ───────────────────────────────────
            await loadLive();

            // ── Notify Sensors + Events tabs ─────────────────────────────
            if (svRefreshFn) svRefreshFn();
            if (evRefreshFn) evRefreshFn();
        };

        refresh();
        setInterval(refresh, 3000);

        // ══════════════════════════════════════════════════════════════════════
        //  Events tab — diagnostics timeline + Tier 1 alert rules management
        // ══════════════════════════════════════════════════════════════════════
        const evPanel = insightsShell.querySelector('[data-ov-panel="events"]');

        if (evPanel) {
            // ── State ──────────────────────────────────────────────────────────
            let evWindow    = "24h";
            let evSeverity  = "";
            let evDeviceKey = "";
            let evLastFetch = 0;

            // ── DOM refs ───────────────────────────────────────────────────────
            const evTimeline    = evPanel.querySelector("[data-ev-timeline]");
            const evEmpty       = evPanel.querySelector("[data-ev-empty]");
            const evRulesList   = evPanel.querySelector("[data-ev-rules-list]");
            const evRulesBadge  = evPanel.querySelector("[data-ev-rules-badge]");
            const evAddBtn      = evPanel.querySelector("[data-ev-add-btn]");
            const evAddForm     = evPanel.querySelector("[data-ev-add-form]");
            const evFormDevice  = evPanel.querySelector("[data-ev-form-device]");
            const evFormMetric  = evPanel.querySelector("[data-ev-form-metric]");
            const evFormCond    = evPanel.querySelector("[data-ev-form-cond]");
            const evFormThresh  = evPanel.querySelector("[data-ev-form-threshold]");
            const evFormSev     = evPanel.querySelector("[data-ev-form-severity]");
            const evFormSave    = evPanel.querySelector("[data-ev-form-save]");
            const evFormCancel  = evPanel.querySelector("[data-ev-form-cancel]");
            const evFilterSev   = evPanel.querySelector("[data-ev-filter-sev]");
            const evFilterDev   = evPanel.querySelector("[data-ev-filter-device]");
            const evWindowBar   = evPanel.querySelector(".ev-window-bar");

            // ── Helpers ────────────────────────────────────────────────────────
            const evFmtTs = (ms) => {
                if (!ms) return "";
                const d = new Date(ms);
                return d.toLocaleDateString([], { month: "short", day: "numeric" }) + "  " +
                       d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
            };
            const evFmtTime = (ms) => {
                if (!ms) return "";
                return new Date(ms).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
            };

            const SEV_TONE = { error: "is-error", critical: "is-critical", warning: "is-warning", info: "is-info", resolved: "is-resolved" };

            // ── Device filter population ───────────────────────────────────────
            const evUpdateDeviceFilter = () => {
                if (!evFilterDev) return;
                const current = evFilterDev.value;
                const opts = ['<option value="">All devices</option>'];
                ovConfigured.forEach((d) => {
                    const k = `${d.source}:${d.device_id}`;
                    opts.push(`<option value="${k}"${k === current ? " selected" : ""}>${d.name || d.device_id}</option>`);
                });
                evFilterDev.innerHTML = opts.join("");
            };

            // ── Alert rules ────────────────────────────────────────────────────
            const evLoadRules = async () => {
                try {
                    const r = await fetch("/api/insights/alert-rules");
                    if (!r.ok) return;
                    const d = await r.json();
                    evRenderRules(d.rules || []);
                } catch (e) {
                    console.warn("[Events] rules fetch failed:", e);
                }
            };

            const evRenderRules = (rules) => {
                if (!evRulesList) return;
                if (evRulesBadge) evRulesBadge.textContent = String(rules.length);
                if (rules.length === 0) {
                    evRulesList.innerHTML = `<p class="ev-rules-empty">No alert rules configured.</p>`;
                    return;
                }
                const SYM = { gt: ">", lt: "<", gte: "≥", lte: "≤", eq: "=" };
                evRulesList.innerHTML = rules.map((rule) => {
                    const on  = rule.enabled;
                    const sev = (rule.severity || "warning").toLowerCase();
                    const sym = SYM[rule.condition] || rule.condition;
                    const expr = `${rule.metric_name} ${sym} ${rule.threshold}`;
                    const label = ovConfigured.find((d) => d.source === rule.source && d.device_id === rule.device_id)?.name || rule.device_id;
                    return `
                        <div class="ev-rule-row" data-rule-id="${rule.id}">
                            <span class="ev-rule-device">${label}</span>
                            <span class="ev-rule-expr">${expr}</span>
                            <span class="ev-rule-sev-pill is-${sev}">${sev}</span>
                            <button class="ev-rule-toggle-btn${on ? " is-on" : ""}" data-rule-toggle="${rule.id}">${on ? "ON" : "OFF"}</button>
                            <button class="ev-rule-del-btn" data-rule-del="${rule.id}" title="Delete">✕</button>
                        </div>`;
                }).join("");

                // Toggle
                evRulesList.querySelectorAll("[data-rule-toggle]").forEach((btn) => {
                    btn.addEventListener("click", async () => {
                        const rid     = Number(btn.getAttribute("data-rule-toggle"));
                        const curr    = btn.classList.contains("is-on");
                        const r       = await fetch(`/api/insights/alert-rules/${rid}`, {
                            method: "PUT",
                            headers: { "Content-Type": "application/json" },
                            body: JSON.stringify({ enabled: !curr }),
                        });
                        if (r.ok) evLoadRules();
                    });
                });
                // Delete
                evRulesList.querySelectorAll("[data-rule-del]").forEach((btn) => {
                    btn.addEventListener("click", async () => {
                        const rid = Number(btn.getAttribute("data-rule-del"));
                        if (!confirm("Delete this alert rule?")) return;
                        const r = await fetch(`/api/insights/alert-rules/${rid}`, { method: "DELETE" });
                        if (r.ok) evLoadRules();
                    });
                });
            };

            // ── Add-rule form ─────────────────────────────────────────────────
            // Populate device select from ovConfigured
            const evPopulateFormDevices = () => {
                if (!evFormDevice) return;
                evFormDevice.innerHTML = ['<option value="">Device…</option>',
                    ...ovConfigured.map((d) => {
                        const k = `${d.source}|${d.device_id}`;
                        return `<option value="${k}">${d.name || d.device_id}</option>`;
                    })
                ].join("");
            };

            if (evFormDevice) {
                evFormDevice.addEventListener("change", () => {
                    if (!evFormMetric) return;
                    const [src, did] = (evFormDevice.value || "").split("|");
                    const device = ovConfigured.find((d) => d.source === src && d.device_id === did);
                    const metrics = device?.expected_metrics || [];
                    evFormMetric.innerHTML = ['<option value="">Metric…</option>',
                        ...metrics.map((m) => `<option value="${m.name}">${displayLabel(m.name)}</option>`)
                    ].join("");
                });
            }

            if (evAddBtn && evAddForm) {
                evAddBtn.addEventListener("click", () => {
                    evPopulateFormDevices();
                    evAddForm.classList.remove("ev-hidden");
                    evAddBtn.classList.add("ev-hidden");
                });
            }
            if (evFormCancel) {
                evFormCancel.addEventListener("click", () => {
                    evAddForm?.classList.add("ev-hidden");
                    evAddBtn?.classList.remove("ev-hidden");
                });
            }
            if (evFormSave) {
                evFormSave.addEventListener("click", async () => {
                    const [src, did] = (evFormDevice?.value || "").split("|");
                    const metric    = evFormMetric?.value;
                    const cond      = evFormCond?.value;
                    const threshold = evFormThresh?.value;
                    const severity  = evFormSev?.value || "warning";
                    if (!src || !did || !metric || !cond || threshold === "") {
                        alert("Fill in all fields."); return;
                    }
                    try {
                        const r = await fetch("/api/insights/alert-rules", {
                            method: "POST",
                            headers: { "Content-Type": "application/json" },
                            body: JSON.stringify({ source: src, device_id: did, metric_name: metric, condition: cond, threshold: Number(threshold), severity }),
                        });
                        const d = await r.json();
                        if (!d.ok) { alert(d.message || "Failed to save."); return; }
                        evAddForm?.classList.add("ev-hidden");
                        evAddBtn?.classList.remove("ev-hidden");
                        if (evFormDevice)  evFormDevice.value  = "";
                        if (evFormMetric)  evFormMetric.innerHTML = '<option value="">Metric…</option>';
                        if (evFormThresh)  evFormThresh.value   = "";
                        evLoadRules();
                    } catch (e) { alert("Network error."); }
                });
            }

            // ── Event timeline ─────────────────────────────────────────────────
            const evLoadEvents = async () => {
                const params = new URLSearchParams({ window: evWindow });
                if (evSeverity)  params.set("severity",  evSeverity);
                if (evDeviceKey) {
                    const [src, ...rest] = evDeviceKey.split(":");
                    params.set("source",    src);
                    params.set("device_id", rest.join(":"));
                }
                try {
                    const r = await fetch(`/api/insights/events?${params}`);
                    if (!r.ok) return;
                    const d = await r.json();
                    evRenderTimeline(d.events || []);
                    evLastFetch = Date.now();
                } catch (e) {
                    console.warn("[Events] events fetch failed:", e);
                }
            };

            const evRenderTimeline = (events) => {
                if (!evTimeline) return;
                if (events.length === 0) {
                    evTimeline.innerHTML = "";
                    evEmpty?.classList.remove("ev-hidden");
                    return;
                }
                evEmpty?.classList.add("ev-hidden");

                evTimeline.innerHTML = events.map((ev) => {
                    const sev   = (ev.severity || "info").toLowerCase();
                    const tone  = SEV_TONE[sev] || "is-info";
                    const etype = (ev.event_type || "").replace("alert:", "⚡ ");
                    const name  = ev.device_name || ev.device_id || "";
                    const msg   = ev.message || "";
                    const count = ev._count || 1;

                    // Compact timestamp: single → "May 2  13:34:08"
                    //                   range  → "13:23:30 → 13:23:55"
                    const tsDisplay = count > 1
                        ? `${evFmtTime(ev._first_ts)} → ${evFmtTime(ev.timestamp_ms)}`
                        : evFmtTs(ev.timestamp_ms);
                    const fullTs = count > 1
                        ? `${evFmtTs(ev._first_ts)}  →  ${evFmtTs(ev.timestamp_ms)}`
                        : evFmtTs(ev.timestamp_ms);
                    const countBadge = count > 1
                        ? `<span class="ev-count-badge">×${count}</span>`
                        : "";

                    return `
                        <div class="ev-event-row ${tone}">
                            <div class="ev-row-top">
                                <span class="ev-sev-badge ${tone}">${sev}</span>
                                <span class="ev-device">${name}</span>
                                <span class="ev-type">${etype}</span>
                            </div>
                            <div class="ev-row-bottom">
                                <span class="ev-ts" title="${fullTs}">${tsDisplay}</span>
                                ${countBadge}
                                <span class="ev-message" title="${msg}">${msg}</span>
                            </div>
                        </div>`;
                }).join("");
            };

            // ── Filter wiring ──────────────────────────────────────────────────
            if (evFilterSev) {
                evFilterSev.addEventListener("change", () => {
                    evSeverity = evFilterSev.value;
                    evLoadEvents();
                });
            }
            if (evFilterDev) {
                evFilterDev.addEventListener("change", () => {
                    evDeviceKey = evFilterDev.value;
                    evLoadEvents();
                });
            }
            if (evWindowBar) {
                evWindowBar.querySelectorAll("[data-ev-win]").forEach((btn) => {
                    btn.addEventListener("click", () => {
                        evWindow = btn.getAttribute("data-ev-win");
                        evWindowBar.querySelectorAll("[data-ev-win]").forEach((b) => b.classList.remove("is-current"));
                        btn.classList.add("is-current");
                        evLastFetch = 0;
                        evLoadEvents();
                    });
                });
            }

            // ── Entry point ────────────────────────────────────────────────────
            const evRefresh = () => {
                const tabBtn   = insightsShell.querySelector('[data-ov-tab="events"]');
                const isActive = tabBtn?.classList.contains("is-current");

                evUpdateDeviceFilter();

                if (!isActive) return;

                // Load rules once per tab visit (they change rarely)
                if (!evRulesList?.children.length) evLoadRules();

                // Poll events every 10 s
                if (!evLastFetch || (Date.now() - evLastFetch) > 10_000) {
                    evLoadEvents();
                }
            };

            evRefreshFn = evRefresh;
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  Data Forwarding page
    // ══════════════════════════════════════════════════════════════════════════
    const fwShell = document.querySelector("[data-forwarding-shell]");
    if (fwShell) {
        // ── DOM refs ────────────────────────────────────────────────────────
        const fwFormCard    = fwShell.querySelector("[data-fw-form]");
        const fwFormTitle   = fwShell.querySelector("[data-fw-form-title]");
        const fwProfileList = fwShell.querySelector("[data-fw-profile-list]");
        const fwEmpty       = fwShell.querySelector("[data-fw-empty]");
        const fwSaveMsg     = fwShell.querySelector("[data-fw-save-msg]");

        const fwFName         = fwShell.querySelector("[data-fw-f-name]");
        const fwFProtocol     = fwShell.querySelector("[data-fw-f-protocol]");
        const fwFEnabled      = fwShell.querySelector("[data-fw-f-enabled]");
        const fwFScope        = fwShell.querySelector("[data-fw-f-scope]");
        // MQTT
        const fwFMqttHost     = fwShell.querySelector("[data-fw-f-mqtt-host]");
        const fwFMqttPort     = fwShell.querySelector("[data-fw-f-mqtt-port]");
        const fwFMqttTls      = fwShell.querySelector("[data-fw-f-mqtt-tls]");
        const fwFMqttCid      = fwShell.querySelector("[data-fw-f-mqtt-client-id]");
        const fwFMqttUser     = fwShell.querySelector("[data-fw-f-mqtt-user]");
        const fwFMqttPass     = fwShell.querySelector("[data-fw-f-mqtt-pass]");
        const fwFMqttQos      = fwShell.querySelector("[data-fw-f-mqtt-qos]");
        const fwFMqttRetain   = fwShell.querySelector("[data-fw-f-mqtt-retain]");
        const fwFMqttInterval = fwShell.querySelector("[data-fw-f-mqtt-interval]");
        const fwMqttTlsBlock  = fwShell.querySelector("[data-fw-mqtt-tls-block]");
        // HTTPS
        const fwFHttpsHost       = fwShell.querySelector("[data-fw-f-https-host]");
        const fwFHttpsPort       = fwShell.querySelector("[data-fw-f-https-port]");
        const fwFHttpsSensorPath = fwShell.querySelector("[data-fw-f-https-sensor-path]");
        const fwFHttpsEventsPath = fwShell.querySelector("[data-fw-f-https-events-path]");
        const fwFHttpsAuth       = fwShell.querySelector("[data-fw-f-https-auth-type]");
        const fwFHttpsAVal       = fwShell.querySelector("[data-fw-f-https-auth-val]");
        const fwFHttpsMtls       = fwShell.querySelector("[data-fw-f-https-mtls]");
        const fwFHttpsInt        = fwShell.querySelector("[data-fw-f-https-interval]");
        const fwFHttpsTout       = fwShell.querySelector("[data-fw-f-https-timeout]");
        const fwAuthValWrap      = fwShell.querySelector("[data-fw-auth-val-wrap]");
        const fwAuthValLbl       = fwShell.querySelector("[data-fw-auth-val-label]");
        const fwHttpsMtlsBlock   = fwShell.querySelector("[data-fw-https-mtls-block]");
        const fwHttpsSensorPrev  = fwShell.querySelector("[data-fw-https-sensor-preview]");
        const fwHttpsEventsPrev  = fwShell.querySelector("[data-fw-https-events-preview]");

        // ── State ────────────────────────────────────────────────────────────
        let fwProfiles   = [];
        let fwEditId     = null;
        let fwEnabledVal = false;
        let fwTlsVal     = false;
        let fwRetainVal  = false;
        let fwMtlsVal    = false;

        // ── Cert widget factory ──────────────────────────────────────────────
        const fwInitCertWidget = (container) => {
            if (!container) return { getValue: () => null, setLoaded: () => {} };
            const fileInput = container.querySelector("[data-fw-cert-file]");
            const textarea  = container.querySelector("[data-fw-cert-val]");
            const status    = container.querySelector("[data-fw-cert-status]");
            const clearBtn  = container.querySelector("[data-fw-cert-clear]");
            const uploadBtn = container.querySelector("[data-fw-cert-btn]");

            let widgetState = "none"; // "none" | "server" | "new" | "cleared"

            const setStatus = (filename) => {
                // state: "none" | "server" | "new" | "cleared"
                if (!status) return;
                if (widgetState === "server") {
                    status.textContent = "✓ cert on device";
                    status.classList.add("is-loaded");
                    clearBtn?.classList.remove("fw-hidden");
                } else if (widgetState === "new" && filename) {
                    status.textContent = `✓ ${filename}`;
                    status.classList.add("is-loaded");
                    clearBtn?.classList.remove("fw-hidden");
                } else if (widgetState === "cleared") {
                    status.textContent = "Will be removed on save";
                    status.classList.remove("is-loaded");
                    clearBtn?.classList.add("fw-hidden");
                } else {
                    status.textContent = "No file";
                    status.classList.remove("is-loaded");
                    clearBtn?.classList.add("fw-hidden");
                }
            };

            uploadBtn?.addEventListener("click", () => fileInput?.click());
            fileInput?.addEventListener("change", () => {
                const file = fileInput.files[0];
                if (!file) return;
                const reader = new FileReader();
                reader.onload = (e) => {
                    widgetState = "new";
                    if (textarea) textarea.value = e.target.result;
                    setStatus(file.name);
                };
                reader.readAsText(file);
                fileInput.value = "";
            });
            clearBtn?.addEventListener("click", () => {
                widgetState = "cleared";
                if (textarea) textarea.value = "";
                setStatus();
            });

            return {
                // null = keep existing  |  "" = delete  |  "<pem>" = new content
                getValue: () => {
                    if (widgetState === "server")  return null;   // keep on device
                    if (widgetState === "cleared") return "";     // delete from device
                    if (widgetState === "new")     return textarea?.value?.trim() || null;
                    return null; // "none"
                },
                // Called when loading a profile that already has a cert on the device
                setLoaded: (isLoaded) => {
                    widgetState = isLoaded ? "server" : "none";
                    if (textarea) textarea.value = "";
                    setStatus();
                },
            };
        };

        // Initialise all cert widgets keyed by data-fw-cert attribute
        const fwCerts = {};
        fwShell.querySelectorAll("[data-fw-cert]").forEach((el) => {
            fwCerts[el.getAttribute("data-fw-cert")] = fwInitCertWidget(el);
        });

        // ── Toggle button helper ─────────────────────────────────────────────
        const fwToggle = (btn, getVal, setVal) => {
            if (!btn) return;
            btn.addEventListener("click", () => {
                setVal(!getVal());
                btn.classList.toggle("is-on", getVal());
                btn.textContent = getVal() ? "ON" : "OFF";
            });
        };

        fwToggle(fwFEnabled, () => fwEnabledVal, (v) => { fwEnabledVal = v; });

        fwToggle(fwFMqttTls, () => fwTlsVal, (v) => {
            fwTlsVal = v;
            if (fwFMqttPort && !fwFMqttPort._userEdited) {
                fwFMqttPort.value = v ? 8883 : 1883;
            }
            fwMqttTlsBlock?.classList.toggle("fw-hidden", !v);
        });
        if (fwFMqttPort) {
            fwFMqttPort.addEventListener("input", () => { fwFMqttPort._userEdited = true; });
        }

        fwToggle(fwFMqttRetain, () => fwRetainVal, (v) => { fwRetainVal = v; });

        fwToggle(fwFHttpsMtls, () => fwMtlsVal, (v) => {
            fwMtlsVal = v;
            fwHttpsMtlsBlock?.classList.toggle("fw-hidden", !v);
        });

        // ── Protocol switch ──────────────────────────────────────────────────
        const fwShowProto = (proto) => {
            fwShell.querySelectorAll("[data-fw-proto]").forEach((el) => {
                el.classList.toggle("fw-hidden", el.getAttribute("data-fw-proto") !== proto);
            });
        };
        fwFProtocol?.addEventListener("change", () => fwShowProto(fwFProtocol.value));

        // ── HTTPS auth type → show/hide auth value ───────────────────────────
        const fwUpdateAuthLabel = () => {
            const t = fwFHttpsAuth?.value;
            fwAuthValWrap?.classList.toggle("fw-hidden", t === "none");
            const labels = { bearer: "Bearer Token", api_key: "API Key", basic: "user:password" };
            if (fwAuthValLbl) fwAuthValLbl.textContent = labels[t] || "Token / Key";
        };
        fwFHttpsAuth?.addEventListener("change", fwUpdateAuthLabel);

        // ── HTTPS URL preview ────────────────────────────────────────────────
        const fwUpdateHttpsPreview = () => {
            const host  = fwFHttpsHost?.value.trim() || "host";
            const port  = Number(fwFHttpsPort?.value) || 443;
            const ps    = port === 443 ? "" : `:${port}`;
            const base  = `https://${host}${ps}`;
            const sp    = fwFHttpsSensorPath?.value.trim();
            const ep    = fwFHttpsEventsPath?.value.trim();
            if (fwHttpsSensorPrev) fwHttpsSensorPrev.textContent = sp ? `${base}${sp}` : "—";
            if (fwHttpsEventsPrev) fwHttpsEventsPrev.textContent = ep ? `${base}${ep}` : "(not configured)";
        };
        [fwFHttpsHost, fwFHttpsPort, fwFHttpsSensorPath, fwFHttpsEventsPath].forEach((el) => {
            el?.addEventListener("input", fwUpdateHttpsPreview);
        });

        // ── Topics info panel toggle ─────────────────────────────────────────
        fwShell.querySelectorAll("[data-fw-topics-toggle]").forEach((btn) => {
            btn.addEventListener("click", () => {
                const body  = fwShell.querySelector("[data-fw-topics-body]");
                const arrow = fwShell.querySelector("[data-fw-topics-arrow]");
                const shown = !body?.classList.contains("fw-hidden");
                body?.classList.toggle("fw-hidden", shown);
                if (arrow) arrow.textContent = shown ? "▼" : "▲";
            });
        });

        // ── Load config ──────────────────────────────────────────────────────
        const fwLoad = async () => {
            try {
                const r = await fetch("/api/forwarding/config");
                if (!r.ok) return;
                const d = await r.json();
                fwProfiles = d.profiles || [];
            } catch (e) {
                console.warn("[Forwarding] load failed:", e);
            }
            fwRender();
        };

        // ── Save config ──────────────────────────────────────────────────────
        const fwSave = async () => {
            if (fwSaveMsg) { fwSaveMsg.textContent = ""; fwSaveMsg.className = "fw-save-msg"; }

            const proto = fwFProtocol?.value || "mqtt";
            const profile = {
                id:       fwEditId || "",
                name:     fwFName?.value.trim() || "Unnamed Profile",
                enabled:  fwEnabledVal,
                protocol: proto,
                scope:    fwFScope?.value || "all",
            };

            if (proto === "mqtt") {
                profile.mqtt = {
                    host:             fwFMqttHost?.value.trim() || "",
                    port:             Number(fwFMqttPort?.value) || 1883,
                    tls:              fwTlsVal,
                    tls_ca:           fwCerts["mqtt-ca"]?.getValue(),
                    tls_cert:         fwCerts["mqtt-cert"]?.getValue(),
                    tls_key:          fwCerts["mqtt-key"]?.getValue(),
                    client_id:        fwFMqttCid?.value.trim()  || "",
                    username:         fwFMqttUser?.value.trim() || "",
                    password:         fwFMqttPass?.value        || "",
                    qos:              Number(fwFMqttQos?.value) || 1,
                    retain:           fwRetainVal,
                    interval_seconds: Number(fwFMqttInterval?.value) || 5,
                };
            } else {
                const hasMtls = fwMtlsVal;
                profile.https = {
                    host:             fwFHttpsHost?.value.trim() || "",
                    port:             Number(fwFHttpsPort?.value) || 443,
                    sensor_path:      fwFHttpsSensorPath?.value.trim() || "/ingest",
                    events_path:      fwFHttpsEventsPath?.value.trim() || "",
                    auth_type:        fwFHttpsAuth?.value || "none",
                    auth_value:       fwFHttpsAVal?.value || "",
                    // null=keep, ""=clear, "<pem>"=new. When mTLS toggled OFF, send "" to clear all.
                    tls_ca:           hasMtls ? fwCerts["https-ca"]?.getValue()   : "",
                    tls_cert:         hasMtls ? fwCerts["https-cert"]?.getValue() : "",
                    tls_key:          hasMtls ? fwCerts["https-key"]?.getValue()  : "",
                    interval_seconds: Number(fwFHttpsInt?.value)  || 30,
                    timeout_seconds:  Number(fwFHttpsTout?.value) || 10,
                };
            }

            if (fwEditId) {
                const idx = fwProfiles.findIndex((p) => p.id === fwEditId);
                if (idx >= 0) fwProfiles[idx] = profile;
                else fwProfiles.push(profile);
            } else {
                fwProfiles.push(profile);
            }

            try {
                const r = await fetch("/api/forwarding/config", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ profiles: fwProfiles }),
                });
                const d = await r.json();
                if (d.ok) {
                    if (fwSaveMsg) fwSaveMsg.textContent = "Saved.";
                    fwHideForm();
                    await fwLoad();
                } else {
                    if (fwSaveMsg) { fwSaveMsg.textContent = d.message || "Save failed."; fwSaveMsg.className = "fw-save-msg is-error"; }
                }
            } catch (e) {
                if (fwSaveMsg) { fwSaveMsg.textContent = "Network error."; fwSaveMsg.className = "fw-save-msg is-error"; }
            }
        };

        // ── Form show / hide ─────────────────────────────────────────────────
        const fwSyncToggleBtn = (btn, val) => {
            if (!btn) return;
            btn.classList.toggle("is-on", val);
            btn.textContent = val ? "ON" : "OFF";
        };

        const fwShowForm = (profile = null) => {
            fwEditId = profile?.id || null;
            if (fwFormTitle) fwFormTitle.textContent = profile ? `Edit: ${profile.name}` : "New Forwarding Profile";

            const proto = profile?.protocol || "mqtt";
            if (fwFProtocol) fwFProtocol.value = proto;
            fwShowProto(proto);

            fwEnabledVal = profile ? !!profile.enabled : false;
            fwSyncToggleBtn(fwFEnabled, fwEnabledVal);

            if (fwFName)  fwFName.value  = profile?.name  || "";
            if (fwFScope) fwFScope.value = profile?.scope || "all";

            if (proto === "mqtt") {
                const m = profile?.mqtt || {};
                if (fwFMqttHost)     fwFMqttHost.value   = m.host || "";
                if (fwFMqttPort)     { fwFMqttPort.value = m.port || 1883; fwFMqttPort._userEdited = false; }
                if (fwFMqttCid)      fwFMqttCid.value    = m.client_id || "";
                if (fwFMqttUser)     fwFMqttUser.value   = m.username  || "";
                if (fwFMqttPass)     fwFMqttPass.value   = m.password  || "";
                if (fwFMqttQos)      fwFMqttQos.value    = String(m.qos ?? 1);
                if (fwFMqttInterval) fwFMqttInterval.value = String(m.interval_seconds || 5);

                fwTlsVal = !!m.tls;
                fwSyncToggleBtn(fwFMqttTls, fwTlsVal);
                fwMqttTlsBlock?.classList.toggle("fw-hidden", !fwTlsVal);

                fwRetainVal = !!m.retain;
                fwSyncToggleBtn(fwFMqttRetain, fwRetainVal);

                // API returns _loaded flags, not PEM content
                fwCerts["mqtt-ca"]?.setLoaded(!!m.tls_ca_loaded);
                fwCerts["mqtt-cert"]?.setLoaded(!!m.tls_cert_loaded);
                fwCerts["mqtt-key"]?.setLoaded(!!m.tls_key_loaded);

            } else {
                const h = profile?.https || {};
                if (fwFHttpsHost)       fwFHttpsHost.value       = h.host         || "";
                if (fwFHttpsPort)       fwFHttpsPort.value       = String(h.port  || 443);
                if (fwFHttpsSensorPath) fwFHttpsSensorPath.value = h.sensor_path  || "/ingest";
                if (fwFHttpsEventsPath) fwFHttpsEventsPath.value = h.events_path  || "";
                if (fwFHttpsAuth)       fwFHttpsAuth.value       = h.auth_type    || "none";
                if (fwFHttpsAVal)       fwFHttpsAVal.value       = h.auth_value   || "";
                if (fwFHttpsInt)        fwFHttpsInt.value        = String(h.interval_seconds || 30);
                if (fwFHttpsTout)       fwFHttpsTout.value       = String(h.timeout_seconds  || 10);

                const hasCerts = !!(h.tls_ca_loaded || h.tls_cert_loaded || h.tls_key_loaded);
                fwMtlsVal = hasCerts;
                fwSyncToggleBtn(fwFHttpsMtls, fwMtlsVal);
                fwHttpsMtlsBlock?.classList.toggle("fw-hidden", !fwMtlsVal);

                fwCerts["https-ca"]?.setLoaded(!!h.tls_ca_loaded);
                fwCerts["https-cert"]?.setLoaded(!!h.tls_cert_loaded);
                fwCerts["https-key"]?.setLoaded(!!h.tls_key_loaded);

                fwUpdateAuthLabel();
                fwUpdateHttpsPreview();
            }

            fwFormCard?.classList.remove("fw-hidden");
            fwFormCard?.scrollIntoView({ behavior: "smooth", block: "nearest" });
        };

        const fwHideForm = () => {
            fwFormCard?.classList.add("fw-hidden");
            fwEditId = null;
        };

        // ── Render profile list ──────────────────────────────────────────────
        const fwRender = () => {
            if (!fwProfileList) return;
            if (fwProfiles.length === 0) {
                fwProfileList.innerHTML = "";
                fwEmpty?.classList.remove("ov-hidden");
                return;
            }
            fwEmpty?.classList.add("ov-hidden");

            fwProfileList.innerHTML = fwProfiles.map((p) => {
                const proto   = p.protocol || "mqtt";
                const enabled = p.enabled;
                let details   = "";

                if (proto === "mqtt") {
                    const m    = p.mqtt || {};
                    const sec  = m.tls ? (m.tls_cert_loaded ? " · mTLS" : " · TLS") : "";
                    const intvl = `every ${m.interval_seconds || 5} s`;
                    details = `
                        <div class="fw-profile-detail">
                            <span class="fw-detail-label">Broker</span>
                            <span class="fw-detail-val">${m.host || "—"}:${m.port || 1883}${sec}</span>
                        </div>
                        <div class="fw-profile-detail">
                            <span class="fw-detail-label">Topics</span>
                            <span class="fw-detail-val">metacrust/{source}/{device_id}/{metric}</span>
                        </div>
                        <div class="fw-profile-detail">
                            <span class="fw-detail-label">QoS</span>
                            <span class="fw-detail-val">${m.qos ?? 1} · retain ${m.retain ? "on" : "off"}</span>
                        </div>
                        <div class="fw-profile-detail">
                            <span class="fw-detail-label">Publish</span>
                            <span class="fw-detail-val">${intvl}</span>
                        </div>`;
                } else {
                    const h     = p.https || {};
                    const port  = Number(h.port) || 443;
                    const ps    = port === 443 ? "" : `:${port}`;
                    const base  = `https://${h.host || "—"}${ps}`;
                    const sec   = h.tls_cert_loaded ? " · mTLS" : h.tls_ca_loaded ? " · custom CA" : "";
                    details = `
                        <div class="fw-profile-detail">
                            <span class="fw-detail-label">Sensor URL</span>
                            <span class="fw-detail-val">${base}${h.sensor_path || "/ingest"}${sec}</span>
                        </div>
                        ${h.events_path ? `
                        <div class="fw-profile-detail">
                            <span class="fw-detail-label">Events URL</span>
                            <span class="fw-detail-val">${base}${h.events_path}</span>
                        </div>` : ""}
                        <div class="fw-profile-detail">
                            <span class="fw-detail-label">Auth</span>
                            <span class="fw-detail-val">${h.auth_type || "none"}</span>
                        </div>
                        <div class="fw-profile-detail">
                            <span class="fw-detail-label">POST</span>
                            <span class="fw-detail-val">every ${h.interval_seconds || 30} s</span>
                        </div>`;
                }

                return `
                    <div class="fw-profile-card ${enabled ? "is-enabled" : "is-disabled"}" data-fw-card="${p.id}">
                        <div class="fw-profile-head">
                            <span class="fw-profile-name">${p.name || "Unnamed"}</span>
                            <span class="fw-proto-badge is-${proto}">${proto === "mqtt" ? "MQTT" : "HTTPS"}</span>
                            <span class="fw-profile-status ${enabled ? "is-on" : "is-off"}">${enabled ? "● Active" : "○ Disabled"}</span>
                            <div class="fw-profile-actions">
                                <button class="fw-action-btn" data-fw-edit="${p.id}">Edit</button>
                                <button class="fw-action-btn is-del" data-fw-del="${p.id}">Delete</button>
                            </div>
                        </div>
                        <div class="fw-profile-body">${details}
                            <div class="fw-profile-detail">
                                <span class="fw-detail-label">Scope</span>
                                <span class="fw-detail-val">${p.scope === "all" ? "All devices" : p.scope}</span>
                            </div>
                        </div>
                    </div>`;
            }).join("");

            fwProfileList.querySelectorAll("[data-fw-edit]").forEach((btn) => {
                btn.addEventListener("click", () => {
                    const id = btn.getAttribute("data-fw-edit");
                    fwShowForm(fwProfiles.find((p) => p.id === id) || null);
                });
            });
            fwProfileList.querySelectorAll("[data-fw-del]").forEach((btn) => {
                btn.addEventListener("click", async () => {
                    const id   = btn.getAttribute("data-fw-del");
                    const prof = fwProfiles.find((p) => p.id === id);
                    if (!confirm(`Delete profile "${prof?.name || id}"?`)) return;
                    fwProfiles = fwProfiles.filter((p) => p.id !== id);
                    await fetch("/api/forwarding/config", {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify({ profiles: fwProfiles }),
                    });
                    fwRender();
                });
            });
        };

        // ── Wire static buttons ──────────────────────────────────────────────
        fwShell.querySelectorAll("[data-fw-add]").forEach((btn) => {
            btn.addEventListener("click", () => fwShowForm(null));
        });
        fwShell.querySelector("[data-fw-cancel]")?.addEventListener("click", fwHideForm);
        fwShell.querySelector("[data-fw-save]")?.addEventListener("click", fwSave);

        // Initial load
        fwLoad();
    }


});