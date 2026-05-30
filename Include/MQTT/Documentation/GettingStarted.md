# Getting Started

This page is the shortest path from a fresh clone to a validated MQTT client checkout.

Start at [Documentation Home](README.md) if you want the full documentation map.

## 1. Put The Repository Where MetaTrader Can See It

You have two supported layouts:

1. Clone the repository so that the repository root is your MetaTrader 5 `MQL5` folder.
2. Clone the repository anywhere, then let the checked-in helpers sync `Include/MQTT`, `Scripts/MQTT`, and `Experts/MQTT` into another `MQL5` folder when you validate.

The public library surface is still the same in both layouts:

```mql5
#include <MQTT\MQTT.mqh>
```

## 2. Confirm The Public Entry Point

A healthy clone should contain these paths:

- `Include/MQTT/MQTT.mqh`
- `Include/MQTT/Documentation/README.md`
- `Experts/MQTT/Harnesses/MinimalClientExample.mq5`
- `Tools/compile-public-validation.ps1`

If those files are present, you have the intended public checkout.

## 3. Compile The Curated Public Validation Set

From the repository root, run:

```powershell
.\Tools\compile-public-validation.ps1
```

If MetaEditor is installed outside the default Windows location, either pass it explicitly:

```powershell
.\Tools\compile-public-validation.ps1 -MetaEditorPath 'C:\Program Files\MetaTrader 5\MetaEditor64.exe'
```

or set `METAEDITOR_PATH` first.

If your active terminal uses a different MT5 data root than the repository checkout, sync into that target root during validation:

```powershell
.\Tools\compile-public-validation.ps1 `
  -TargetMql5Root 'D:\MT5\MQL5' `
  -SyncRepoToTarget
```

Expected result: the helper reports `PASS` for the curated compile targets and each target compiles with `0 errors, 0 warnings`.

## 4. Optional Runtime Bring-Up After Compile

The compile-first path is the supported public green path. The tracked repository does not currently ship a one-command live broker smoke helper or a checked-in live broker smoke harness source file.

If you need broker-specific runtime verification after the compile gate passes, start from [MinimalClientExample.mq5](../../../Experts/MQTT/Harnesses/MinimalClientExample.mq5) and keep broker hosts, credentials, certificates, and launch details in local or ignored state instead of tracked files.

Before any runtime test, add the broker host to `Tools -> Options -> Expert Advisors -> Allow WebRequest for listed URL`. MT5 still requires that allowlist entry before socket connections are permitted.

## 5. Know Which Checked-In Example To Start With

- [MinimalClientExample.mq5](../../../Experts/MQTT/Harnesses/MinimalClientExample.mq5): smallest timer-driven client example.
- [PublishQueueTestHarness.mq5](../../../Experts/MQTT/Harnesses/PublishQueueTestHarness.mq5): queue durability and retransmit-oriented runtime harness.

For a side-by-side comparison and extra snippets, use [Examples Guide](Examples.md).

## 6. If You Want An IDE Agent To Drive Validation

Use [IDE Automation Guide](AutomationGuide.md). It provides the exact inputs an IDE agent needs and prompt templates that work against the checked-in scripts without editing tracked secrets into the repository.

## See Also

- [Validation Guide](Validation.md)
- [Examples Guide](Examples.md)
- [IDE Automation Guide](AutomationGuide.md)
- [Troubleshooting](Troubleshooting.md)
- [OnTimer Guide](OnTimerGuide.md)
