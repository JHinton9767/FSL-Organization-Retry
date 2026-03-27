let
    RosterFolderPath = Excel.CurrentWorkbook(){[Name="RosterFolderPath"]}[Content]{0}[Column1],
    Source = Folder.Files(RosterFolderPath),
    KeepVisible = Table.SelectRows(Source, each [Attributes]?[Hidden]? <> true),
    KeepSupported = Table.SelectRows(KeepVisible, each List.Contains({".csv", ".xlsx", ".xls"}, Text.Lower([Extension]))),
    AddContentTable = Table.AddColumn(
        KeepSupported,
        "Data",
        each
            if Text.Lower([Extension]) = ".csv"
            then Csv.Document([Content], [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv])
            else Excel.Workbook([Content], true)
    ),
    ExpandExcelSheets = Table.AddColumn(
        AddContentTable,
        "NormalizedTables",
        each
            if Text.Lower([Extension]) = ".csv"
            then Table.PromoteHeaders([Data], [PromoteAllScalars=true])
            else
                let
                    SheetsOnly = Table.SelectRows([Data], each [Kind] = "Sheet" or [Kind] = "Table"),
                    Combined = Table.Combine(List.Transform(SheetsOnly[Data], each Table.PromoteHeaders(_, [PromoteAllScalars=true])))
                in
                    Combined
    ),
    SelectData = Table.SelectColumns(ExpandExcelSheets, {"Name", "NormalizedTables"}),
    FirstTableColumns = if Table.RowCount(SelectData) > 0 then Table.ColumnNames(SelectData{0}[NormalizedTables]) else {},
    Expanded = Table.ExpandTableColumn(SelectData, "NormalizedTables", FirstTableColumns, FirstTableColumns),
    AddSourceFile = Table.AddColumn(Expanded, "source_file", each [Name], type text)
in
    AddSourceFile
