import { useState } from 'react';
import { Card, CardContent } from './ui/card';
import { Button } from './ui/button';
import { Input } from './ui/input';
import { Label } from './ui/label';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription, DialogFooter } from './ui/dialog';
import { Checkbox } from './ui/checkbox';
import { RadioGroup, RadioGroupItem } from './ui/radio-group';

const warehouses = [
  'Москва, МО и Дальние регионы',
  'Санкт-Петербург и СЗО',
  'Юг',
  'Сибирь',
  'Воронеж',
  'Урал',
  'Казань',
  'Ярославль',
  'Уфа',
  'Кавказ',
  'Дальний Восток',
  'Красноярск',
  'Саратов',
  'Самара',
  'Тюмень',
  'Беларусь',
  'Калининград',
  'Казахстан',
  'Узбекистан',
  'Армения',
  'Кыргызстан',
  'Грузия',
  'Азербайджан',
  'Без кластера',
];

const productNames = [
  'MATCHA,ЦЕНТР-НС',
  'DEFIANT №74',
  'Интрига жидкие гвозди',
  'Покрытие для садовых дорожек',
  'Аэрозоль JOLLY',
  'Аэрозоль Fénix Gloss',
  'Быстросохнущее покрытие',
  'Бронза МЦ-91T',
  'Краска акриловая',
  'Эмаль алкидная',
  'Грунтовка универсальная',
  'Лак паркетный',
  'Шпатлевка финишная',
  'Клей монтажный',
  'Герметик силиконовый',
  'Пена монтажная',
  'Растворитель',
  'Олифа натуральная',
  'Антисептик для дерева',
  'Пропитка влагозащитная',
];

// Функция генерации случайного числа
const getRandomQuantity = () => Math.floor(Math.random() * 200);

// Генерация mock данных для 1000 товаров
const generateMockData = () => {
  const data = [];
  for (let i = 1; i <= 1000; i++) {
    const productName = productNames[Math.floor(Math.random() * productNames.length)];
    const warehouseQuantities = warehouses.map(() => getRandomQuantity());
    
    data.push({
      id: i,
      name: `${productName} №${i}`,
      warehouses: warehouseQuantities,
    });
  }
  return data;
};

const mockData = generateMockData();

// Склады для отгрузки (2-3 основных склада)
const shipmentWarehouses = [
  'Склад Москва',
  'Склад Санкт-Петербург',
  'Склад Казань',
];

export function ShipmentTable() {
  const [selectedRows, setSelectedRows] = useState<Set<number>>(new Set());
  const [rangeFrom, setRangeFrom] = useState('');
  const [rangeTo, setRangeTo] = useState('');
  const [dialogOpen, setDialogOpen] = useState(false);
  const [dialogStep, setDialogStep] = useState(1);
  const [selectedSupplyWarehouses, setSelectedSupplyWarehouses] = useState<Set<string>>(new Set(warehouses));
  const [selectedShipmentWarehouse, setSelectedShipmentWarehouse] = useState('');

  const handleRowClick = (rowIndex: number) => {
    setSelectedRows(prev => {
      const newSet = new Set(prev);
      if (newSet.has(rowIndex)) {
        newSet.delete(rowIndex);
      } else {
        newSet.add(rowIndex);
      }
      return newSet;
    });
  };

  const handleSelectRange = () => {
    const from = parseInt(rangeFrom);
    const to = parseInt(rangeTo);
    
    if (isNaN(from) || isNaN(to) || from < 1 || to > mockData.length || from > to) {
      return;
    }

    setSelectedRows(prev => {
      const newSet = new Set(prev);
      for (let i = from; i <= to; i++) {
        newSet.add(i);
      }
      return newSet;
    });
  };

  const handleCreateShipment = () => {
    setDialogOpen(true);
    setDialogStep(1);
  };

  const handleReset = () => {
    setSelectedRows(new Set());
    setRangeFrom('');
    setRangeTo('');
  };

  const handleSelectAllWarehouses = () => {
    setSelectedSupplyWarehouses(new Set(warehouses));
  };

  const handleDeselectAllWarehouses = () => {
    setSelectedSupplyWarehouses(new Set());
  };

  const handleToggleWarehouse = (warehouse: string) => {
    setSelectedSupplyWarehouses(prev => {
      const newSet = new Set(prev);
      if (newSet.has(warehouse)) {
        newSet.delete(warehouse);
      } else {
        newSet.add(warehouse);
      }
      return newSet;
    });
  };

  const handleStep1Next = () => {
    if (selectedSupplyWarehouses.size > 0) {
      setDialogStep(2);
    }
  };

  const handleStep2Back = () => {
    setDialogStep(1);
  };

  const handleStep2Next = () => {
    if (selectedShipmentWarehouse) {
      console.log('Создание отгрузки:', {
        rows: Array.from(selectedRows).sort((a, b) => a - b),
        supplyWarehouses: Array.from(selectedSupplyWarehouses),
        shipmentWarehouse: selectedShipmentWarehouse,
      });
      setDialogOpen(false);
      setDialogStep(1);
      setSelectedShipmentWarehouse('');
    }
  };

  const handleDialogClose = () => {
    setDialogOpen(false);
    setDialogStep(1);
    setSelectedShipmentWarehouse('');
  };

  return (
    <div className="space-y-3">
      <div className="flex items-end gap-4">
        <div className="flex items-end gap-2">
          <div className="space-y-1">
            <Label htmlFor="range-from" className="text-xs">С строки</Label>
            <Input
              id="range-from"
              type="number"
              min="1"
              max={mockData.length}
              value={rangeFrom}
              onChange={(e) => setRangeFrom(e.target.value)}
              className="w-24 h-8 text-xs"
              placeholder="10"
            />
          </div>
          <div className="space-y-1">
            <Label htmlFor="range-to" className="text-xs">По строку</Label>
            <Input
              id="range-to"
              type="number"
              min="1"
              max={mockData.length}
              value={rangeTo}
              onChange={(e) => setRangeTo(e.target.value)}
              className="w-24 h-8 text-xs"
              placeholder="35"
            />
          </div>
          <Button 
            onClick={handleSelectRange} 
            variant="outline" 
            size="sm"
            className="h-8"
          >
            Выделить
          </Button>
          {selectedRows.size > 0 && (
            <Button 
              onClick={handleReset} 
              variant="outline" 
              size="sm"
              className="h-8"
            >
              Сброс
            </Button>
          )}
        </div>
        
        {selectedRows.size > 0 && (
          <div className="flex items-center gap-3 ml-auto">
            <span className="text-xs text-gray-600">
              Выбрано строк: {selectedRows.size}
            </span>
            <Button 
              onClick={handleCreateShipment}
              className="h-8 bg-green-600 hover:bg-green-700"
            >
              Сформировать отгрузку
            </Button>
          </div>
        )}
      </div>

      <Card className="overflow-hidden rounded-t-lg">
        <CardContent className="p-0">
          <div className="overflow-auto" style={{ maxHeight: 'calc(100vh - 140px)' }}>
            <table className="w-full border-collapse text-xs">
              <thead className="sticky top-0 z-20 bg-white shadow-sm">
                <tr className="border-b">
                  <th className="sticky left-0 top-0 bg-white z-30 min-w-[60px] w-[60px] border-r py-2 px-3 text-center">
                    №
                  </th>
                  <th className="sticky left-[60px] top-0 bg-white z-30 min-w-[300px] border-r-2 py-2 px-3 text-left">
                    Название артикула
                  </th>
                  {warehouses.map((warehouse, index) => (
                    <th key={index} className="text-center min-w-[140px] whitespace-nowrap bg-white py-2 px-3">
                      {warehouse}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {mockData.map((row, rowIndex) => {
                  const displayRowNumber = rowIndex + 1;
                  const isSelected = selectedRows.has(displayRowNumber);
                  
                  return (
                    <tr 
                      key={row.id} 
                      onClick={() => handleRowClick(displayRowNumber)}
                      className={`border-b cursor-pointer transition-colors ${
                        isSelected
                          ? 'bg-green-50' 
                          : 'hover:bg-gray-50'
                      }`}
                    >
                      <td className={`sticky left-0 z-10 border-r py-1.5 px-3 text-center ${
                        isSelected ? 'bg-green-50' : 'bg-white'
                      }`}>
                        {displayRowNumber}
                      </td>
                      <td className={`sticky left-[60px] z-10 border-r-2 py-1.5 px-3 ${
                        isSelected ? 'bg-green-50' : 'bg-white'
                      }`}>
                        {row.name}
                      </td>
                      {row.warehouses.map((quantity, index) => (
                        <td 
                          key={index} 
                          className="text-center py-1.5 px-3"
                        >
                          {quantity}
                        </td>
                      ))}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>

      <Dialog open={dialogOpen} onOpenChange={handleDialogClose}>
        <DialogContent className="max-w-2xl max-h-[80vh] overflow-y-auto">
          {dialogStep === 1 && (
            <>
              <DialogHeader>
                <DialogTitle>Выберите склады для поставки</DialogTitle>
                <DialogDescription>
                  Выберите один или несколько складов, на которые будет осуществлена поставка товаров
                </DialogDescription>
              </DialogHeader>
              
              <div className="space-y-4">
                <div className="flex gap-2">
                  <Button 
                    onClick={handleSelectAllWarehouses} 
                    variant="outline" 
                    size="sm"
                  >
                    Выделить все
                  </Button>
                  <Button 
                    onClick={handleDeselectAllWarehouses} 
                    variant="outline" 
                    size="sm"
                  >
                    Снять все
                  </Button>
                </div>

                <div className="grid grid-cols-2 gap-3">
                  {warehouses.map((warehouse) => (
                    <div key={warehouse} className="flex items-center space-x-2">
                      <Checkbox
                        id={`warehouse-${warehouse}`}
                        checked={selectedSupplyWarehouses.has(warehouse)}
                        onCheckedChange={() => handleToggleWarehouse(warehouse)}
                      />
                      <label
                        htmlFor={`warehouse-${warehouse}`}
                        className="text-sm cursor-pointer"
                      >
                        {warehouse}
                      </label>
                    </div>
                  ))}
                </div>
              </div>

              <DialogFooter>
                <Button onClick={handleDialogClose} variant="outline">
                  Отменить
                </Button>
                <Button 
                  onClick={handleStep1Next}
                  disabled={selectedSupplyWarehouses.size === 0}
                >
                  Далее
                </Button>
              </DialogFooter>
            </>
          )}

          {dialogStep === 2 && (
            <>
              <DialogHeader>
                <DialogTitle>Выберите склад для отгрузки</DialogTitle>
                <DialogDescription>
                  Выберите склад, с которого будет осуществлена отгрузка товаров
                </DialogDescription>
              </DialogHeader>

              <RadioGroup 
                value={selectedShipmentWarehouse} 
                onValueChange={setSelectedShipmentWarehouse}
              >
                <div className="space-y-3">
                  {shipmentWarehouses.map((warehouse) => (
                    <div key={warehouse} className="flex items-center space-x-2">
                      <RadioGroupItem value={warehouse} id={`shipment-${warehouse}`} />
                      <Label 
                        htmlFor={`shipment-${warehouse}`}
                        className="cursor-pointer"
                      >
                        {warehouse}
                      </Label>
                    </div>
                  ))}
                </div>
              </RadioGroup>

              <DialogFooter>
                <Button onClick={handleStep2Back} variant="outline">
                  Вернуться назад
                </Button>
                <Button onClick={handleDialogClose} variant="outline">
                  Отменить
                </Button>
                <Button 
                  onClick={handleStep2Next}
                  disabled={!selectedShipmentWarehouse}
                >
                  Далее
                </Button>
              </DialogFooter>
            </>
          )}
        </DialogContent>
      </Dialog>
    </div>
  );
}
