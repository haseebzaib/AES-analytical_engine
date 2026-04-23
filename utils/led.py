import logging
import subprocess

logger = logging.getLogger(__name__)

_state = "off"


def toggle_led() -> None:
    """Blink user2 LED on the CM5 IO board — signals AES is alive."""
    global _state
    _state = "on" if _state == "off" else "off"
    try:
        subprocess.run(
            ["/opt/gateway/scripts/gateway-cm5-ioctl", "user2", _state],
            check=False,
            capture_output=True,
            timeout=2,
        )
    except Exception as exc:
        logger.debug("LED toggle skipped: %s", exc)
