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

    const systemShell = document.querySelector("[data-system-shell]");
    if (systemShell) {
        const tabs = Array.from(systemShell.querySelectorAll("[data-system-tab]"));
        const panels = Array.from(systemShell.querySelectorAll("[data-system-panel]"));
        const wifiModeSelect = systemShell.querySelector("[data-wifi-mode-select]");

        const syncWifiMode = (mode) => {
            const modeSections = Array.from(systemShell.querySelectorAll("[data-wifi-mode-visible]"));
            modeSections.forEach((section) => {
                const visible = section.getAttribute("data-wifi-mode-visible") === mode;
                section.classList.toggle("is-hidden", !visible);
            });
        };

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

        if (wifiModeSelect instanceof HTMLSelectElement) {
            syncWifiMode(wifiModeSelect.value);
            wifiModeSelect.addEventListener("change", () => syncWifiMode(wifiModeSelect.value));
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

        const wifiForm = systemShell.querySelector("[data-wifi-form]");
        const wifiMessage = systemShell.querySelector("[data-wifi-message]");
        if (wifiForm instanceof HTMLFormElement && wifiMessage) {
            wifiForm.addEventListener("submit", async (event) => {
                event.preventDefault();
                wifiMessage.textContent = "";

                const formData = new FormData(wifiForm);
                const payload = {
                    interface: String(formData.get("interface") || "wlan0"),
                    enabled: formData.get("enabled") === "on",
                    mode: String(formData.get("mode") || "client"),
                    auto_start: formData.get("auto_start") === "on",
                    country_code: String(formData.get("country_code") || ""),
                    band: String(formData.get("band") || "auto"),
                    channel: String(formData.get("channel") || "auto"),
                    channel_width: String(formData.get("channel_width") || "20"),
                    hidden_ssid: formData.get("hidden_ssid") === "on",
                    ssid: String(formData.get("ssid") || ""),
                    security: String(formData.get("security") || "wpa2-psk"),
                    password: String(formData.get("password") || ""),
                    client_dhcp: formData.get("client_dhcp") === "on",
                    client_address: String(formData.get("client_address") || ""),
                    client_gateway: String(formData.get("client_gateway") || ""),
                    client_dns: String(formData.get("client_dns") || ""),
                    route_metric: String(formData.get("route_metric") || ""),
                    access_point_address: String(formData.get("access_point_address") || ""),
                    access_point_dhcp_server: formData.get("access_point_dhcp_server") === "on",
                    access_point_dhcp_range_start: String(formData.get("access_point_dhcp_range_start") || ""),
                    access_point_dhcp_range_end: String(formData.get("access_point_dhcp_range_end") || ""),
                    share_uplink: formData.get("share_uplink") === "on",
                    uplink_interface: String(formData.get("uplink_interface") || "eth0"),
                };

                const button = wifiForm.querySelector('button[type="submit"]');
                if (button instanceof HTMLButtonElement) {
                    button.disabled = true;
                    button.textContent = "Saving...";
                }

                try {
                    const response = await fetch("/api/system/wifi", {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify(payload),
                    });
                    const data = await response.json();
                    wifiMessage.textContent = data.message || "Saved.";
                    wifiMessage.classList.toggle("is-success", response.ok && data.ok);
                    if (!response.ok || !data.ok) {
                        throw new Error(data.message || "Could not save Wi-Fi profile.");
                    }
                } catch (error) {
                    wifiMessage.textContent = error instanceof Error ? error.message : "Could not save Wi-Fi profile.";
                    wifiMessage.classList.remove("is-success");
                } finally {
                    if (button instanceof HTMLButtonElement) {
                        button.disabled = false;
                        button.textContent = "Save Wi-Fi profile";
                    }
                }
            });
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
});
