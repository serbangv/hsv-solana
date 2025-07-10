# About

This is a mod of the Solana Agave validator that allows a hot-spare running in proximity to the primary validator to assist in voting.


❗️ This is a proof of concept, not production-ready, although it did run on mainnet for 16 epochs without issues.

## Concept

Because most mainnet operators already run a hot-spare alongside their primary validator, this mod lets the spare join the voting process and step in when the primary lags, boosting TVC performance.

## Overview
1.	The primary validator runs an independent secondary Tower.
2.	Both primary and hot-spare submit votes to it; the first vote that arrives and passes the secondary Tower check is broadcast.


## How to run

#### 1. On the primary server
- Add this CLI arg to your startup script:
```bash
--hsv-listen-port <PORT> # the port that listens for vote transactions coming from the hot-spare
```
- Add a rule in your firewall to allow incoming UDP from the hot-spare’s IP

#### 2. On the hot-spare
- Add these CLI args to your startup script:

```bash
--hsv-identity <KEYPAIR>      # path to the primary validator’s identity keypair
--hsv-send-to <IP:PORT>       # primary’s IP and hsv-listen-port
--hsv-vote-account <PUBKEY>   # primary validator’s vote account pubkey
```

Once it's running, check the validator logs for lines containing `hot_spare_vote` to monitor.

## Does it work?

In my tests, running this mod would reliably result in better latency than without, which should result in better TVC scores.

## Note
Works only if the vote account’s authorized voter is the validator’s identity keypair.
