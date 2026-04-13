document.addEventListener("DOMContentLoaded", () => {
    const form = document.querySelector(".auth-form");

    if (!form) {
        return;
    }

    form.addEventListener("submit", (event) => {
        event.preventDefault();
    });
});
