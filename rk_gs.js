const ABC_UPDATE_URL = "http://188.68.222.242:9006/api/ozon/analytics/abc/update";

function openOverlay() {
  const html = HtmlService.createHtmlOutputFromFile('OverlayOnly')
    .setWidth(420).setHeight(200);
  SpreadsheetApp.getUi().showModalDialog(html, 'Обновление ABC-аналитики');
}

// Серверная обёртка: возвращаем результат вместо toast
function runAbcUpdate() {
  const payload = {
    spreadsheet_url: SpreadsheetApp.getActive().getUrl()
  };
  const options = {
    method: "post",
    contentType: "application/json",
    payload: JSON.stringify(payload),
    followRedirects: true,
    muteHttpExceptions: true
  };

  try {
    const res  = UrlFetchApp.fetch(ABC_UPDATE_URL, options);
    const code = res.getResponseCode();
    const text = res.getContentText();
    const ok   = (code === 200 || code === 202);
    return { ok, code, text };
  } catch (e) {
    return { ok: false, code: -1, text: String(e) };
  }
}

/** ================== НАСТРОЙКИ ================== */
const ENDPOINT_URL = 'http://188.68.222.242:9006/api/ozon/createorupdateads/'; 
const ADD_HEADERS = { /* 'Authorization': 'Bearer XXX' */ };

function openDialog() {
  const html = HtmlService.createHtmlOutputFromFile('OneDialog')
    .setWidth(520).setHeight(260);
  SpreadsheetApp.getUi().showModalDialog(html, 'РК Ozon');
}

function runCampaign() {
  const params = {
    method: 'get',    
    headers: ADD_HEADERS,
    muteHttpExceptions: true,
    followRedirects: true
  };
  const resp = UrlFetchApp.fetch(ENDPOINT_URL, params);
  const code = resp.getResponseCode();
  const body = resp.getContentText();
  return { ok: code >= 200 && code < 300, code, body };
}



const TOGGLE_URL = "http://188.68.222.242:9006/api/ozon/ads/toggle/";

// открыть форму с подтверждением
function openToggleDialog() {
  const sh = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Main_ADV"); // поменяй имя листа если другое
  const status = (sh.getRange("S3").getValue() || "").toString().trim();
  const storeName = (sh.getRange("V23").getValue() || "").toString().trim();

  const msg = status === "Включен"
    ? "Сейчас РК работают автоматически.<br><br><b>После нажатия «Продолжить» они будут остановлены.</b>"
    : "Сейчас все автоматические РК остановлены.<br><br><b>После нажатия «Продолжить» они будут запущены.</b>";

  const html = HtmlService.createTemplateFromFile("ToggleDialog");
  html.message = msg;
  html.storeName = storeName;
  html.prevStatus = status; // <-- передаем в шаблон
  SpreadsheetApp.getUi().showModalDialog(
    html.evaluate().setWidth(450).setHeight(220),
    "Переключение автозапуска РК"
  );
}

// серверный вызов
function runToggle(storeName) {
  const payload = { store_name: storeName };
  const options = {
    method: "post",
    contentType: "application/json",
    payload: JSON.stringify(payload),
    followRedirects: true,
    muteHttpExceptions: true
  };
  try {
    const res = UrlFetchApp.fetch(TOGGLE_URL, options);
    const code = res.getResponseCode();
    const text = res.getContentText();
    return { ok: code >= 200 && code < 300, code, text };
  } catch (e) {
    return { ok: false, code: -1, text: String(e) };
  }
}

