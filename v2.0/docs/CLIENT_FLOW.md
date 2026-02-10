# JV Matchmaker Platform: Client Flow

## Overview Diagram

```mermaid
flowchart TB
    subgraph ENTRY["Entry Points"]
        A1[New User] --> B1[Create Profile]
        A2[Guest User] --> B2[Enter Candidate Info]
        A3[Existing User] --> B3[Browse Directory]
    end

    subgraph PROFILE["Profile Setup"]
        B1 --> C1[Name, Company, Email]
        C1 --> C2[Niche & Industry]
        C2 --> C3[Seeking & Offering]
        C3 --> C4[List Size & Audience]
        C4 --> D1[Profile Complete]
    end

    subgraph GUEST["Guest Candidate Flow"]
        B2 --> G1[Enter: Name, Company]
        G1 --> G2[Enter: Niche, Seeking, Offering]
        G2 --> G3[Enter: List Size, Who They Serve]
        G3 --> G4[Run Match Search]
    end

    subgraph MATCHING["Matching Engine"]
        D1 --> M1[ISMC Scoring]
        G4 --> M2[Directory Matching]
        B3 --> M3[Browse & Filter]

        M1 --> M4{{"Score Calculation"}}
        M2 --> M4

        M4 --> M5[Intent 45%]
        M4 --> M6[Synergy 25%]
        M4 --> M7[Momentum 20%]
        M4 --> M8[Context 10%]

        M5 & M6 & M7 & M8 --> M9[Harmonic Mean]
        M9 --> M10[Bidirectional Score]
    end

    subgraph RESULTS["Match Results"]
        M10 --> R1[Match List]
        M3 --> R1
        R1 --> R2[Partner Cards]
        R2 --> R3[Match Score]
        R2 --> R4[Why Good Fit]
        R2 --> R5[Suggested Approach]
        R2 --> R6[Conversation Starter]
    end

    subgraph ACTIONS["User Actions"]
        R2 --> A_VIEW[View Profile Detail]
        R2 --> A_SAVE[Save Partner]
        R2 --> A_EXPORT[Export to CSV]
        R2 --> A_DISMISS[Dismiss]

        A_VIEW --> P1[Full Partner Analysis]
        P1 --> P2[Partnership Insights]
        P2 --> P3[Tier: Hand-Picked / Strong / Wildcard]
    end

    subgraph OUTREACH["Outreach & Tracking"]
        P3 --> O1[Copy Conversation Starter]
        O1 --> O2[Send Email/LinkedIn]
        O2 --> O3[Update Status]

        O3 --> S1[Pending]
        S1 --> S2[Contacted]
        S2 --> S3[In Progress]
        S3 --> S4[Connected]
        S3 --> S5[Declined]
    end

    subgraph OUTCOME["Partnership Outcome"]
        S4 --> OUT1[Partnership Formed]
        OUT1 --> OUT2[Track Results]
        OUT2 --> OUT3[Feedback Loop]
        OUT3 -.-> M4
    end

    style ENTRY fill:#e1f5fe
    style MATCHING fill:#fff3e0
    style RESULTS fill:#e8f5e9
    style ACTIONS fill:#f3e5f5
    style OUTREACH fill:#fce4ec
    style OUTCOME fill:#e0f2f1
```

---

## Simplified Linear Flow

```mermaid
flowchart LR
    A[Enter Platform] --> B[Create/View Profile]
    B --> C[Get Matched]
    C --> D[Review Matches]
    D --> E[Reach Out]
    E --> F[Form Partnership]
    F --> G[Track Outcome]
```

---

## User Journey by Persona

### Persona 1: JV Directory Member

```mermaid
flowchart TD
    M1[Member logs in] --> M2[Views pre-computed matches]
    M2 --> M3[Filters by niche/list size]
    M3 --> M4[Reviews top 10 matches]
    M4 --> M5[Exports match report]
    M5 --> M6[Sends personalized outreach]
    M6 --> M7[Tracks responses]
```

### Persona 2: Chelsea's Client (Guest Matching)

```mermaid
flowchart TD
    C1[Chelsea enters client info] --> C2[Runs match against 3,143+ partners]
    C2 --> C3[Gets top 100 scored matches]
    C3 --> C4[Exports CSV with outreach scripts]
    C4 --> C5[Client uses scripts for outreach]
    C5 --> C6[Optionally: Save candidate to directory]
```

### Persona 3: Partner Discovery

```mermaid
flowchart TD
    P1[User browses directory] --> P2[Searches by niche/keyword]
    P2 --> P3[Filters: Member status, list size]
    P3 --> P4[Views partner profile]
    P4 --> P5[Sees: Why good fit + suggested approach]
    P5 --> P6[Initiates contact]
```

---

## Matching Algorithm Flow

```mermaid
flowchart TB
    subgraph INPUT["Input Data"]
        I1[User Profile]
        I2[Partner Profile]
        I3[ICP Data]
    end

    subgraph SCORING["ISMC Scoring"]
        S1["Intent (45%)<br/>Collaboration history<br/>LinkedIn presence<br/>Contact availability"]
        S2["Synergy (25%)<br/>Audience alignment<br/>Industry match<br/>Content style"]
        S3["Momentum (20%)<br/>Profile freshness<br/>Activity level<br/>Growth potential"]
        S4["Context (10%)<br/>Completeness<br/>Data quality<br/>Network proximity"]
    end

    subgraph CALCULATION["Score Calculation"]
        C1[Weight each dimension]
        C2[Calculate harmonic mean]
        C3[Score A→B direction]
        C4[Score B→A direction]
        C5[Combine bidirectionally]
    end

    subgraph OUTPUT["Match Output"]
        O1[Final Score 0-100]
        O2[Match Reason]
        O3[Why Good Fit]
        O4[Suggested Approach]
        O5[Conversation Starter]
        O6[Tier Classification]
    end

    I1 & I2 & I3 --> S1 & S2 & S3 & S4
    S1 & S2 & S3 & S4 --> C1
    C1 --> C2
    C2 --> C3 & C4
    C3 & C4 --> C5
    C5 --> O1 & O2 & O3 & O4 & O5 & O6
```

---

## Data Flow Architecture

```mermaid
flowchart TB
    subgraph SOURCES["Data Sources"]
        DB1[(Supabase<br/>3,143+ Partners)]
        DB2[(Django DB<br/>User Profiles)]
        DB3[(Clay API<br/>Enrichment)]
    end

    subgraph ENGINE["Matching Engine"]
        E1[MatchScoringService]
        E2[CandidateDirectoryMatchService]
        E3[PartnershipAnalyzer]
    end

    subgraph STORAGE["Results Storage"]
        R1[(Match Model)]
        R2[(SupabaseMatch)]
        R3[Session Cache]
    end

    subgraph OUTPUT["User Output"]
        O1[Profile List View]
        O2[Match Report]
        O3[CSV Export]
        O4[PDF Report*]
    end

    DB1 --> E1 & E2 & E3
    DB2 --> E1
    DB3 --> E1

    E1 --> R1
    E2 --> R3
    E3 --> R2

    R1 & R2 & R3 --> O1 & O2 & O3 & O4

    style O4 stroke-dasharray: 5 5
```

*PDF Report = planned feature from competitive enhancements

---

## Status Tracking Flow

```mermaid
stateDiagram-v2
    [*] --> New: Match Generated
    New --> Viewed: User Opens
    Viewed --> Contacted: Outreach Sent
    Contacted --> InProgress: Response Received
    InProgress --> Connected: Partnership Formed
    InProgress --> Declined: No Fit
    Viewed --> Dismissed: Not Interested
    Connected --> [*]
    Declined --> [*]
    Dismissed --> [*]
```

---

## Future State: With Competitive Enhancements

```mermaid
flowchart TB
    subgraph CURRENT["Current Flow"]
        A[Match Generated] --> B[User Reviews]
        B --> C[Manual Outreach]
        C --> D[Track Status]
    end

    subgraph ENHANCED["Enhanced Flow"]
        E[Match Generated] --> F{Mutual Match?}
        F -->|Yes| G[Hot Match Alert]
        F -->|No| H[Regular Match]
        G --> I[Request Intro]
        H --> I
        I --> J{Both Requested?}
        J -->|Yes| K[Auto Email Intro]
        J -->|No| L[Manual Outreach]
        K --> M[Track Outcome]
        L --> M
        M --> N[Feedback Loop]
        N -.-> E
    end

    style ENHANCED fill:#e8f5e9
```

---

## Quick Reference: URL Structure

| URL | Purpose |
|-----|---------|
| `/partners/` | Browse 3,143+ directory |
| `/partners/<id>/` | View partner detail |
| `/partners/<id>/matches-report/` | Show matches for partner |
| `/find-matches/` | Guest candidate form |
| `/find-matches/report/` | Guest matching results |
| `/saved-candidates/` | Manage saved candidates |
| `/profiles/` | User's own profiles |
| `/matches/` | User's calculated matches |
