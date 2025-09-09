/**
 * Экспорт артикулов по городам в XLSX (города берём из 2-й строки).
 * Лист: "Отбор для поставок"
 * Диапазон артикулов:
 *   AA12 — начать с (включительно)
 *   AA13 — закончить (включительно)
 * Для каждого города создаётся файл "<Город>.xlsx" в папке "Экспорт поставок".
 * Старые файлы перезаписываются. Требуется Advanced Drive Service (Drive API).
 */
function exportTopNPerCity() {
    const SHEET_NAME = "Отбор для поставок";
    const FOLDER_NAME = "Экспорт поставок";
    const START_CELL = "AA12";
    const END_CELL   = "AA13";
    const THROTTLE_MS = 150; // небольшая пауза между экспортами
  
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheet = ss.getSheetByName(SHEET_NAME);
    if (!sheet) throw new Error(`Лист "${SHEET_NAME}" не найден`);
  
    const startN = Number(sheet.getRange(START_CELL).getValue() || 0);
    const endN   = Number(sheet.getRange(END_CELL).getValue() || 0);
    if (!startN || !endN || startN < 1 || endN < startN) {
      throw new Error(`Укажи корректный диапазон в ${START_CELL} (начать с) и ${END_CELL} (закончить)`);
    }
  
    // Читаем фактическую область
    const lastRow = sheet.getLastRow();
    const lastCol = sheet.getLastColumn();
    const data = sheet.getRange(1, 1, lastRow, lastCol).getValues();
  
    // ----- ВАЖНО: названия городов берём ИЗ 2-Й СТРОКИ -----
    const citiesRow = data[1] || [];
    const cities = citiesRow.slice(2).map(v => String(v || "").trim()); // C:… — города
    if (!cities.length) throw new Error("Во 2-й строке не найдены названия городов.");
  
    // Данные товаров начинаются С 3-Й СТРОКИ
    const rows = data.slice(2);
  
    // Сортировка по сумме (колонка B) и выбор поддиапазона [startN..endN]
    const sorted = rows
      .filter(r => r && r[0] && !isNaN(Number(r[1]))) // есть артикул и числовая сумма
      .sort((a, b) => Number(b[1] || 0) - Number(a[1] || 0));
    const selected = sorted.slice(startN - 1, endN);
  
    // Папка назначения
    const targetFolder = getOrCreateFolderByName_(FOLDER_NAME);
  
    // Одна временная Google-таблица на все города
    const tmpSS = SpreadsheetApp.create("TMP_EXPORT_PER_CITY");
    const tmpSheet = tmpSS.getSheets()[0];
    tmpSheet.setName("Export");
  
    for (let ci = 0; ci < cities.length; ci++) {
      const cityName = cities[ci];
      if (!cityName) continue; // пропускаем пустые «города»
  
      const cityCol = ci + 2; // индекс столбца города в selected (A=0,B=1,C=2,...)
      const exportRows = [["Артикул", "имя (необязательно)", "Количество"]];
  
      for (let r = 0; r < selected.length; r++) {
        const qty = Number(selected[r][cityCol] || 0);
        if (qty > 0) exportRows.push([String(selected[r][0]), "", qty]);
      }
  
      if (exportRows.length > 1) {
        tmpSheet.clearContents();
        tmpSheet.setName(safeSheetName_(cityName));
        tmpSheet.getRange(1, 1, exportRows.length, exportRows[0].length).setValues(exportRows);
  
        // Даем Шитсу зафиксировать данные перед экспортом
        SpreadsheetApp.flush();
        Utilities.sleep(120);
  
        const fileName = `${safeFileName_(cityName)}.xlsx`;
  
        // Экспорт XLSX (Drive v2 с getBlob, fallback на Drive v3)
        const blob = exportSpreadsheetToXlsx_(tmpSS.getId(), fileName);
  
        // Перезапись в целевой папке
        deleteExistingFileIfAny_(targetFolder, fileName);
        targetFolder.createFile(blob);
  
        Utilities.sleep(THROTTLE_MS);
      }
    }
  
    // Удаляем временную таблицу
    DriveApp.getFileById(tmpSS.getId()).setTrashed(true);
  
    SpreadsheetApp.getUi().alert(
      `Готово! Сформированы файлы по артикулу ${startN}–${endN} в папке "${FOLDER_NAME}".`
    );
  }
  
  /** Экспортирует Spreadsheet в XLSX и возвращает Blob (Drive v2 → v3 fallback) */
  function exportSpreadsheetToXlsx_(spreadsheetId, desiredName) {
    const mimeXlsx = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
    try {
      const resp = Drive.Files.export(spreadsheetId, mimeXlsx, { alt: "media" });
      return resp.getBlob().setName(desiredName);
    } catch (e1) {
      const url = "https://www.googleapis.com/drive/v3/files/" +
        encodeURIComponent(spreadsheetId) +
        "/export?mimeType=" + encodeURIComponent(mimeXlsx);
      const res = UrlFetchApp.fetch(url, {
        method: "get",
        headers: { Authorization: "Bearer " + ScriptApp.getOAuthToken() },
        muteHttpExceptions: true
      });
      if (res.getResponseCode() !== 200) {
        throw new Error("Export failed: " + res.getContentText());
      }
      return res.getBlob().setName(desiredName);
    }
  }
  
  /** Перезапись: удаляет файл(ы) с указанным именем в папке */
  function deleteExistingFileIfAny_(folder, name) {
    const it = folder.getFilesByName(name);
    while (it.hasNext()) it.next().setTrashed(true);
  }
  
  /** Папка по имени или создание новой */
  function getOrCreateFolderByName_(name) {
    const it = DriveApp.getFoldersByName(name);
    return it.hasNext() ? it.next() : DriveApp.createFolder(name);
  }
  
  /** Безопасное имя файла (для файловой системы) */
  function safeFileName_(s) {
    return String(s).replace(/[\\/:*?"<>|]/g, "_").trim();
  }
  
  /** Безопасное имя листа (для Excel, ≤31 символ, запретные символы) */
  function safeSheetName_(s) {
    const name = String(s).replace(/[:\\\/\?\*\[\]]/g, "_").trim();
    return (name || "Лист1").substring(0, 31);
  }
  