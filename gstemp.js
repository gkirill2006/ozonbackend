function updateOzonAnalytics() {
    const sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
    
    // Получаем данные из столбца AE для исключения товаров
    const excludeOfferIds = getExcludeOfferIds(sheet);
    
    // Получаем данные обязательных товаров из столбцов AF-AG
    const mandatoryProducts = getMandatoryProducts(sheet);
    
    const params = {
      days: sheet.getRange("B6").getValue(),
      period_analiz: sheet.getRange("B5").getValue(),
      b7: sheet.getRange("B7").getValue(),
      sort_by_qty : sheet.getRange("F8").getValue(),
      f9 : sheet.getRange("F9").getValue(),
      "Api-Key": sheet.getRange("B9").getValue(),
      "client_id": sheet.getRange("B8").getValue(),
      price_min: sheet.getRange("F5").getValue(),
      price_max: sheet.getRange("G5").getValue(),
      sklad_max: sheet.getRange("G6").getValue(),
      f6 : sheet.getRange("F6").getValue(),
      g6 : sheet.getRange("G6").getValue(),
      f7 : sheet.getRange("F7").getValue(),
      f10 : sheet.getRange("F10").getValue(),
      exclude_offer_ids: excludeOfferIds,
      mandatory_products: mandatoryProducts
    };
  
    const payload = {};
    for (const [key, value] of Object.entries(params)) {
      if (value !== "" && value !== null) {
        payload[key] = value;
      }
    }
    
    // Очищаем старые данные одним вызовом
    sheet.getRange("A16:AC10000").clearContent().clearFormat();
  
    const url = "http://188.68.222.242:9006/api/ozon/analytics/";
    const headers = {
      "Content-Type": "application/json",
    };
  
    const options = {
      method: "post",
      headers: headers,
      payload: JSON.stringify(payload),
      muteHttpExceptions: true,
    };
  
    const response = UrlFetchApp.fetch(url, options);
    const json = JSON.parse(response.getContentText());
  
    // Подготавливаем все данные для batch записи
    const allData = [];
    const allFormulas = [];
    const allBackgrounds = [];
    const rowHeights = [];
    
    let row = 16;
  
    for (const cluster of json.clusters) {
      const { cluster_name, cluster_share_percent, cluster_revenue, products } = cluster;
  
      for (const product of products) {
        // Подготавливаем данные для строки
        const rowData = [
          '', // A - будет заполнено формулой
          product.offer_id,
          product.price,
          product.barcodes[0],
          product.category,
          product.type_name,
          '', // G - будет заполнено формулой
          product.fbs_stock_total_qty,
          product.sales_total_fbo_fbs,
          product.product_total_revenue_fbo_fbs,
          product.avg_daily_sales_fbo_fbs,
          product.oborachivaemost,
          cluster_name,
          product.average_delivery_time,
          product.impact_share,
          product.average_delivery_time_item,
          product.impact_share_item,
          product.recommended_supply_item,
          product.payout_total,
          product.sales_qty_cluster,
          product.avg_daily_sales_cluster_rub,
          product.avg_daily_sales_cluster_qty,
          product.share_of_total_daily_average,
          product.stock_total_cluster,
          product.need_goods,
          product.for_delivery
        ];
        
        allData.push(rowData);
        
        // Подготавливаем формулы
        allFormulas.push({
          row: row,
          colA: `=IMAGE("${product.photo}"; 1)`,
          colG: `=HYPERLINK("${product.ozon_link}"; "Ссылка")`
        });
        
        // Подготавливаем цвет фона для столбца Z (26)
        allBackgrounds.push({
          row: row,
          col: 26,
          color: "#FFFF00"
        });
        
        // Высота строки
        rowHeights.push({
          row: row,
          height: 100
        });
        
        row++;
      }
    }
    
    // Batch запись всех данных
    if (allData.length > 0) {
      const range = sheet.getRange(16, 1, allData.length, allData[0].length);
      range.setValues(allData);
    }
    
    // Batch установка формул
    for (const formula of allFormulas) {
      sheet.getRange(formula.row, 1).setFormula(formula.colA);
      sheet.getRange(formula.row, 7).setFormula(formula.colG);
    }
    
    // Batch установка цветов фона
    for (const bg of allBackgrounds) {
      sheet.getRange(bg.row, bg.col).setBackground(bg.color);
    }
    
    // Batch установка высоты строк
    for (const height of rowHeights) {
      sheet.setRowHeight(height.row, height.height);
    }
    
    // Обработка summary данных
    const summaryData = [];
    const summaryBackgrounds = [];
    let row_summary = 16;
    
    for (const summary of json.summary) {
      const bgColor = summary.total_for_delivery > 0 ? "#D9EAD3" : "#F4CCCC";
      
      summaryData.push([
        summary.offer_id,
        summary.barcode,
        summary.total_for_delivery
      ]);
      
      // Подготавливаем цвета фона для summary
      for (let col = 27; col <= 29; col++) {
        summaryBackgrounds.push({
          row: row_summary,
          col: col,
          color: bgColor
        });
      }
      
      row_summary++;
    }
    
    // Batch запись summary данных
    if (summaryData.length > 0) {
      const summaryRange = sheet.getRange(16, 27, summaryData.length, 3);
      summaryRange.setValues(summaryData);
    }
    
    // Batch установка цветов фона для summary
    for (const bg of summaryBackgrounds) {
      sheet.getRange(bg.row, bg.col).setBackground(bg.color);
    }
  
    sheet.getRange(7, 14).setValue(json.average_delivery_time);       
  
    // Статус обновления
    const now = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "HH:mm:ss");
    const date = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "dd.MM.yyyy");
  
    // Статус
    const statusRange = sheet.getRange("J4:M4");
    statusRange.merge();
  
    statusRange.setValue(
    `Статус отчета: Обновление успешно\nTime: ${now}\n${date}\nВремя выполнения запроса на сервере: ${json.execution_time_seconds} сек`
  );
    statusRange.setBackground("#b6d7a8");
  }
  
  /**
   * Получает артикулы товаров для исключения из столбца AF
   * @param {Sheet} sheet - Активный лист
   * @return {Array} Массив артикулов для исключения
   */
  function getExcludeOfferIds(sheet) {
    const excludeOfferIds = [];
    let row = 16;
    
    // Читаем данные из столбца AF начиная с ячейки AF16
    while (true) {
      const cellValue = sheet.getRange(`AF${row}`).getValue();
      
      // Если ячейка пустая, прекращаем чтение
      if (cellValue === "" || cellValue === null || cellValue === undefined) {
        break;
      }
      
      // Преобразуем значение в строку и разделяем по запятой
      const cellString = String(cellValue).trim();
      if (cellString) {
        const items = cellString.split(';').map(item => item.trim()).filter(item => item !== '');
        excludeOfferIds.push(...items);
      }
      
      row++;
    }
    
    // Удаляем дубликаты
    const uniqueOfferIds = [...new Set(excludeOfferIds)];
    
    
    return uniqueOfferIds;
  }
  
    /**
   * Получает обязательные товары из столбцов AG-AH
   * @param {Sheet} sheet - Активный лист
   * @return {Array} Массив объектов с артикулом и количеством
   */
  function getMandatoryProducts(sheet) {
    const mandatoryProducts = [];
    let row = 16;
    
    // Читаем данные из столбцов AG (артикул) и AH (количество) начиная с ячейки AG16
    while (true) {
      const offerId = sheet.getRange(`AG${row}`).getValue();
      const quantity = sheet.getRange(`AH${row}`).getValue();
      
      // Если ячейка с артикулом пустая, прекращаем чтение
      if (offerId === "" || offerId === null || offerId === undefined) {
        break;
      }
      
      // Проверяем, что количество тоже указано
      if (quantity !== "" && quantity !== null && quantity !== undefined && quantity > 0) {
        mandatoryProducts.push({
          offer_id: String(offerId).trim(),
          quantity: parseInt(quantity)
        });
      }
      
      row++;
    }
    
    
    
    return mandatoryProducts;
  }
  