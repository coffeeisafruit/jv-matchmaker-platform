          # JV Matchmaker – Client Flow

Visual map of how users move through the app. Render the Mermaid diagrams in GitHub, [Mermaid Live](https://mermaid.live), or any Mermaid-supported viewer.

---

## High-level flow

```mermaid
flowchart TB
    Start([Landing / Login]) --> Dashboard[Dashboard]
    Dashboard --> Partners[Partners]
    Dashboard --> Positioning[Positioning]
    Dashboard --> Outreach[Outreach]
    Dashboard --> Playbook[Playbook]
    Partners --> InDir[Candidate in directory?]
    InDir -->|Yes| Browse[Browse partner detail]
    Browse --> Report1[Match report + CSV]
    InDir -->|No| FindMatches[Find matches form]
    FindMatches --> Report2[Guest match report]
    Report2 --> AddDir[Add to directory]
    Report2 --> Saved[Saved candidates]
    FindMatches --> Saved
    Saved --> RunReport[Run report again]
    Saved --> AddDir
    Saved --> Edit[Edit / Delete]
    AddDir --> Browse
```

---

## Partner discovery & match report (detailed)

```mermaid
flowchart LR
    subgraph entry [Entry]
        A[Partners page]
    end
    subgraph in_directory [Candidate IN directory]
        B[Search / open partner]
        C[Partner detail]
        D[View full report]
        E[Download CSV]
        B --> C
        C --> D
        C --> E
    end
    subgraph not_in_directory [Candidate NOT in directory]
        F[Find matches]
        G[Enter candidate details]
        H[Find matches submit]
        I[Guest match report]
        J[Add to directory]
        K[Saved candidates list]
        F --> G
        G --> H
        H --> I
        I --> J
        H --> K
        K --> F
    end
    A --> B
    A --> F
    J --> C
```

---

## Guest candidate flow (find matches → report → save / add)

```mermaid
flowchart TB
    A[Partners: Find matches] --> B[Guest candidate form]
    B --> C[Enter name, seeking, offering, niche, etc.]
    C --> D[Submit: Find matches]
    D --> E[SavedCandidate created in DB]
    D --> F[Match service runs vs directory]
    E --> G[Redirect to report]
    F --> G
    G --> H[Guest match report]
    H --> I{Next action}
    I -->|Add to directory| J[SupabaseProfile created]
    I -->|Download CSV| K[CSV download]
    I -->|Try another| B
    I -->|Saved candidates| L[Saved candidates list]
    J --> M[Partner detail page]
    L --> N[Run report / Add to directory / Edit / Delete]
    N --> B
    N --> J
```

---

## Key screens and URLs

| Screen | URL | Purpose |
|--------|-----|---------|
| Partners | `/matching/partners/` | Browse directory, “Find matches”, “Saved candidates” |
| Partner detail | `/matching/partners/<uuid>/` | One partner; “View full report”, “Download CSV” |
| In-directory report | `/matching/partners/<uuid>/matches-report/` | Pre-computed matches for that partner |
| Find matches (form) | `/matching/find-matches/` | Enter guest candidate; submit saves to DB + runs match |
| Guest report | `/matching/find-matches/report/` | On-the-fly match report; “Add to directory”, CSV |
| Saved candidates | `/matching/saved-candidates/` | List of saved candidates |
| Saved detail | `/matching/saved-candidates/<id>/` | Run report, Add to directory, Edit, Delete |

---

## Data flow

```mermaid
flowchart LR
    subgraph input [User input]
        Form[Guest form]
    end
    subgraph persist [Database]
        Saved[SavedCandidate]
        Profiles[(profiles)]
    end
    subgraph compute [Compute]
        Service[CandidateDirectoryMatchService]
    end
    subgraph output [Output]
        Report[Report HTML]
        CSV[CSV]
        PartnerDetail[Partner detail]
    end
    Form --> Saved
    Form --> Service
    Saved --> Service
    Service --> Report
    Report --> CSV
    AddDir[Add to directory] --> Profiles
    AddDir --> Saved
    Profiles --> PartnerDetail
```

- **Guest form** → always creates/updates **SavedCandidate** and runs **CandidateDirectoryMatchService**.
- **Report** comes from session (guest) or from DB (in-directory).
- **Add to directory** creates a **SupabaseProfile** and links it from **SavedCandidate** (`added_to_directory`).
