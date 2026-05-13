#!/usr/bin/env bash
set -euo pipefail

LOG="[qr-scanner-setup]"

DEVICE_BY_ID="/dev/serial/by-id/usb-TMS_Virtual_ComPort_in_FS_Mode_1234567890abcd-if00"
RULE_FILE="/etc/udev/rules.d/99-qr-scanner.rules"
GROUP="plugdev"

echo "$LOG starting..."

# ----------------------------
# 1. Check device exists
# ----------------------------
if [[ ! -e "$DEVICE_BY_ID" ]]; then
    echo "$LOG ERROR: device not found"
    echo "$LOG expected: $DEVICE_BY_ID"
    echo "$LOG try: ls /dev/serial/by-id/"
    exit 1
fi

echo "$LOG found device: $DEVICE_BY_ID"

# ----------------------------
# 2. Resolve USB identity via udev
# ----------------------------
echo "$LOG resolving USB identity..."

UDEV_PROPS=$(udevadm info -q property -n "$DEVICE_BY_ID")

VENDOR=$(echo "$UDEV_PROPS" | grep '^ID_VENDOR_ID=' | cut -d= -f2 || true)
PRODUCT=$(echo "$UDEV_PROPS" | grep '^ID_MODEL_ID=' | cut -d= -f2 || true)

if [[ -z "$VENDOR" || -z "$PRODUCT" ]]; then
    echo "$LOG ERROR: failed to extract USB identity"
    echo "$LOG debugging command:"
    echo "udevadm info -q property -n \"$DEVICE_BY_ID\""
    exit 1
fi

echo "$LOG USB identity: vendor=$VENDOR product=$PRODUCT"

# ----------------------------
# 3. Ensure group exists
# ----------------------------
if ! getent group "$GROUP" >/dev/null; then
    echo "$LOG creating group: $GROUP"
    sudo groupadd "$GROUP"
else
    echo "$LOG group exists: $GROUP"
fi

# ----------------------------
# 4. Add user to group (idempotent)
# ----------------------------
if id -nG "$USER" | grep -qw "$GROUP"; then
    echo "$LOG user already in $GROUP"
else
    echo "$LOG adding user $USER to $GROUP"
    sudo usermod -aG "$GROUP" "$USER"
    echo "$LOG NOTE: log out and back in required"
fi

# ----------------------------
# 5. Install udev rule (idempotent)
# ----------------------------
RULE="SUBSYSTEM==\"tty\", ATTRS{idVendor}==\"$VENDOR\", ATTRS{idProduct}==\"$PRODUCT\", MODE=\"0660\", GROUP=\"$GROUP\""

if [[ -f "$RULE_FILE" ]] && grep -q "$VENDOR" "$RULE_FILE"; then
    echo "$LOG udev rule already exists"
else
    echo "$LOG writing udev rule: $RULE_FILE"
    echo "$RULE" | sudo tee "$RULE_FILE" >/dev/null
fi

# ----------------------------
# 6. Reload udev
# ----------------------------
echo "$LOG reloading udev rules..."
sudo udevadm control --reload-rules
sudo udevadm trigger

# ----------------------------
# 7. Final verification
# ----------------------------
echo "$LOG verifying access..."

DEVNODE=$(readlink -f "$DEVICE_BY_ID")

echo "$LOG resolved device: $DEVNODE"

if [[ -r "$DEVNODE" ]]; then
    echo "$LOG SUCCESS: device is readable without sudo"
else
    echo "$LOG WARNING: device still not readable"
    echo "$LOG debug:"
    echo "ls -l $DEVNODE"
    echo "groups"
    exit 1
fi
