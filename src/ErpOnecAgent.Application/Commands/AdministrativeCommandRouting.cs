namespace ErpOnecAgent.Application.Commands;

public static class AdministrativeCommandRouting
{
    public static bool IsAdministrativeCommandType(string? commandType) => commandType is
        "start_full_sync" or
        "reload_entity" or
        "reconcile_keys" or
        "reconcile_totals" or
        "pause_etl" or
        "resume_etl" or
        "collect_diagnostics" or
        "rotate_certificate_hint" or
        "run_connectivity_test";
}
