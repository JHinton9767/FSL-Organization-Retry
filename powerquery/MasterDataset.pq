let
    Academic = AcademicFolderTransform,
    Rosters = RosterFolderTransform,
    AcademicRenamed = Table.TransformColumnNames(Academic, each Text.Lower(Text.Replace(Text.Replace(_, " ", "_"), "-", "_"))),
    RosterRenamed = Table.TransformColumnNames(Rosters, each Text.Lower(Text.Replace(Text.Replace(_, " ", "_"), "-", "_"))),
    AcademicPrepared = Table.TransformColumnTypes(AcademicRenamed, {{"student_id", type text}, {"email", type text}, {"term", type text}}),
    RosterPrepared = Table.TransformColumnTypes(RosterRenamed, {{"student_id", type text}, {"email", type text}, {"semester_joined", type text}}),
    RosterGrouped = Table.Group(
        RosterPrepared,
        {"student_id"},
        {
            {"chapter", each try List.Last(List.RemoveNulls([chapter])) otherwise null, type text},
            {"membership_status", each try List.Last(List.RemoveNulls([membership_status])) otherwise null, type text},
            {"join_term", each try List.First(List.RemoveNulls([semester_joined])) otherwise null, type text}
        }
    ),
    Joined = Table.NestedJoin(AcademicPrepared, {"student_id"}, RosterGrouped, {"student_id"}, "Roster", JoinKind.LeftOuter),
    Expanded = Table.ExpandTableColumn(Joined, "Roster", {"chapter", "membership_status", "join_term"}, {"chapter", "greek_status", "join_term"})
in
    Expanded
