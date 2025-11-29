import { useState } from 'react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Separator } from '@/components/ui/separator';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import Icon from '@/components/ui/icon';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Textarea } from '@/components/ui/textarea';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { toast } from 'sonner';

type UserRole = 'none' | 'client' | 'courier';
type OrderStatus = 'pending' | 'accepted' | 'completed' | 'cancelled';

interface Order {
  id: string;
  clientName: string;
  address: string;
  description: string;
  price: number;
  status: OrderStatus;
  courierName?: string;
  rating?: number;
  review?: string;
}

const Index = () => {
  const [userRole, setUserRole] = useState<UserRole>('none');
  const [view, setView] = useState<string>('main');
  
  const [orders, setOrders] = useState<Order[]>([
    {
      id: '1',
      clientName: 'Иван Петров',
      address: 'ул. Ленина, д. 45, кв. 12',
      description: 'Вывоз строительного мусора (3 мешка)',
      price: 1500,
      status: 'pending',
    },
    {
      id: '2',
      clientName: 'Мария Сидорова',
      address: 'пр. Мира, д. 78, кв. 5',
      description: 'Старая мебель (диван, стол)',
      price: 2500,
      status: 'pending',
    },
  ]);

  const [courierOrders, setCourierOrders] = useState<Order[]>([]);
  const [completedOrders, setCompletedOrders] = useState<Order[]>([]);
  const [clientOrders, setClientOrders] = useState<Order[]>([]);

  const [newOrder, setNewOrder] = useState({
    address: '',
    description: '',
    price: '',
  });

  const handleCreateOrder = () => {
    if (!newOrder.address || !newOrder.description || !newOrder.price) {
      toast.error('Заполните все поля');
      return;
    }

    const order: Order = {
      id: Date.now().toString(),
      clientName: 'Вы',
      address: newOrder.address,
      description: newOrder.description,
      price: Number(newOrder.price),
      status: 'pending',
    };

    setOrders([...orders, order]);
    setClientOrders([...clientOrders, order]);
    setNewOrder({ address: '', description: '', price: '' });
    toast.success('Заказ создан');
    setView('client-active');
  };

  const handleAcceptOrder = (orderId: string) => {
    const order = orders.find(o => o.id === orderId);
    if (order) {
      const acceptedOrder = { ...order, status: 'accepted' as OrderStatus, courierName: 'Вы' };
      setOrders(orders.filter(o => o.id !== orderId));
      setCourierOrders([...courierOrders, acceptedOrder]);
      toast.success('Заказ принят');
    }
  };

  const handleCompleteOrder = (orderId: string) => {
    const order = courierOrders.find(o => o.id === orderId);
    if (order) {
      const completedOrder = { ...order, status: 'completed' as OrderStatus };
      setCourierOrders(courierOrders.filter(o => o.id !== orderId));
      setCompletedOrders([...completedOrders, completedOrder]);
      toast.success('Заказ завершён');
    }
  };

  const handleRateOrder = (orderId: string, rating: number, review: string) => {
    setCompletedOrders(completedOrders.map(o => 
      o.id === orderId ? { ...o, rating, review } : o
    ));
    toast.success('Отзыв отправлен');
  };

  const renderMainMenu = () => (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 to-gray-100 flex items-center justify-center p-4">
      <Card className="w-full max-w-md shadow-2xl animate-fade-in">
        <CardHeader className="text-center space-y-2 pb-8">
          <div className="mx-auto bg-primary/10 w-20 h-20 rounded-full flex items-center justify-center mb-4">
            <Icon name="Truck" size={40} className="text-primary" />
          </div>
          <CardTitle className="text-3xl font-bold text-primary">Экономь время</CardTitle>
          <CardDescription className="text-base">Курьерская служба вывоза мусора</CardDescription>
        </CardHeader>
        <CardContent className="space-y-3">
          <Button 
            className="w-full h-14 text-lg hover-scale" 
            onClick={() => { setUserRole('courier'); setView('courier-menu'); }}
          >
            <Icon name="Briefcase" size={20} className="mr-2" />
            Стать курьером
          </Button>
          <Button 
            className="w-full h-14 text-lg hover-scale" 
            variant="outline"
            onClick={() => { setUserRole('client'); setView('client-menu'); }}
          >
            <Icon name="User" size={20} className="mr-2" />
            Для клиентов
          </Button>
          <Separator className="my-4" />
          <Button 
            className="w-full h-12 hover-scale" 
            variant="ghost"
            onClick={() => setView('reviews')}
          >
            <Icon name="Star" size={20} className="mr-2" />
            Отзывы
          </Button>
          <Button 
            className="w-full h-12 hover-scale" 
            variant="ghost"
            onClick={() => window.open('https://t.me/support', '_blank')}
          >
            <Icon name="MessageCircle" size={20} className="mr-2" />
            Поддержка
          </Button>
        </CardContent>
      </Card>
    </div>
  );

  const renderCourierMenu = () => (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 to-gray-100 p-4">
      <div className="max-w-4xl mx-auto space-y-4 animate-fade-in">
        <div className="flex items-center justify-between mb-6">
          <Button variant="ghost" onClick={() => { setView('main'); setUserRole('none'); }}>
            <Icon name="ArrowLeft" size={20} className="mr-2" />
            Назад
          </Button>
          <div className="flex items-center gap-2">
            <Badge variant="default" className="text-sm px-3 py-1">
              <Icon name="Briefcase" size={16} className="mr-1" />
              Курьер
            </Badge>
          </div>
        </div>

        <Tabs defaultValue="available" className="w-full">
          <TabsList className="grid w-full grid-cols-4 mb-4">
            <TabsTrigger value="available">Доступные</TabsTrigger>
            <TabsTrigger value="current">Текущие</TabsTrigger>
            <TabsTrigger value="history">История</TabsTrigger>
            <TabsTrigger value="stats">Статистика</TabsTrigger>
          </TabsList>

          <TabsContent value="available" className="space-y-4">
            {orders.map((order) => (
              <Card key={order.id} className="hover-scale">
                <CardHeader>
                  <div className="flex justify-between items-start">
                    <div>
                      <CardTitle className="text-lg">{order.address}</CardTitle>
                      <CardDescription>{order.description}</CardDescription>
                    </div>
                    <Badge className="bg-green-500 text-white">{order.price} ₽</Badge>
                  </div>
                </CardHeader>
                <CardContent>
                  <Button className="w-full" onClick={() => handleAcceptOrder(order.id)}>
                    <Icon name="Check" size={18} className="mr-2" />
                    Принять заказ
                  </Button>
                </CardContent>
              </Card>
            ))}
            {orders.length === 0 && (
              <Card>
                <CardContent className="text-center py-12 text-muted-foreground">
                  <Icon name="Package" size={48} className="mx-auto mb-4 opacity-50" />
                  <p>Нет доступных заказов</p>
                </CardContent>
              </Card>
            )}
          </TabsContent>

          <TabsContent value="current" className="space-y-4">
            {courierOrders.map((order) => (
              <Card key={order.id} className="hover-scale border-primary">
                <CardHeader>
                  <div className="flex justify-between items-start">
                    <div>
                      <CardTitle className="text-lg">{order.address}</CardTitle>
                      <CardDescription>{order.description}</CardDescription>
                    </div>
                    <Badge className="bg-blue-500 text-white">{order.price} ₽</Badge>
                  </div>
                </CardHeader>
                <CardContent className="space-y-2">
                  <div className="flex items-center text-sm text-muted-foreground">
                    <Icon name="User" size={16} className="mr-2" />
                    {order.clientName}
                  </div>
                  <Button className="w-full" onClick={() => handleCompleteOrder(order.id)}>
                    <Icon name="CheckCircle2" size={18} className="mr-2" />
                    Завершить заказ
                  </Button>
                </CardContent>
              </Card>
            ))}
            {courierOrders.length === 0 && (
              <Card>
                <CardContent className="text-center py-12 text-muted-foreground">
                  <Icon name="ClipboardList" size={48} className="mx-auto mb-4 opacity-50" />
                  <p>Нет текущих заказов</p>
                </CardContent>
              </Card>
            )}
          </TabsContent>

          <TabsContent value="history" className="space-y-4">
            {completedOrders.map((order) => (
              <Card key={order.id}>
                <CardHeader>
                  <div className="flex justify-between items-start">
                    <div>
                      <CardTitle className="text-lg">{order.address}</CardTitle>
                      <CardDescription>{order.description}</CardDescription>
                    </div>
                    <Badge variant="outline" className="text-green-600">{order.price} ₽</Badge>
                  </div>
                </CardHeader>
                <CardContent>
                  {order.rating && (
                    <div className="flex items-center gap-1 text-sm text-yellow-500">
                      {[...Array(5)].map((_, i) => (
                        <Icon key={i} name={i < order.rating! ? "Star" : "Star"} size={16} className={i < order.rating! ? "fill-current" : "opacity-30"} />
                      ))}
                      {order.review && <span className="ml-2 text-muted-foreground">"{order.review}"</span>}
                    </div>
                  )}
                </CardContent>
              </Card>
            ))}
            {completedOrders.length === 0 && (
              <Card>
                <CardContent className="text-center py-12 text-muted-foreground">
                  <Icon name="History" size={48} className="mx-auto mb-4 opacity-50" />
                  <p>История заказов пуста</p>
                </CardContent>
              </Card>
            )}
          </TabsContent>

          <TabsContent value="stats">
            <Card>
              <CardHeader>
                <CardTitle>Финансовая статистика</CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="grid grid-cols-2 gap-4">
                  <div className="bg-primary/10 p-4 rounded-lg">
                    <p className="text-sm text-muted-foreground">Всего заказов</p>
                    <p className="text-3xl font-bold text-primary">{completedOrders.length}</p>
                  </div>
                  <div className="bg-green-500/10 p-4 rounded-lg">
                    <p className="text-sm text-muted-foreground">Заработано</p>
                    <p className="text-3xl font-bold text-green-600">
                      {completedOrders.reduce((sum, o) => sum + o.price, 0)} ₽
                    </p>
                  </div>
                </div>
                <Separator />
                <div className="space-y-2">
                  <div className="flex items-center justify-between">
                    <span className="text-sm text-muted-foreground">Средний чек</span>
                    <span className="font-semibold">
                      {completedOrders.length > 0 
                        ? Math.round(completedOrders.reduce((sum, o) => sum + o.price, 0) / completedOrders.length)
                        : 0} ₽
                    </span>
                  </div>
                  <div className="flex items-center justify-between">
                    <span className="text-sm text-muted-foreground">Средний рейтинг</span>
                    <div className="flex items-center gap-1">
                      <Icon name="Star" size={16} className="text-yellow-500 fill-current" />
                      <span className="font-semibold">
                        {completedOrders.length > 0
                          ? (completedOrders.filter(o => o.rating).reduce((sum, o) => sum + (o.rating || 0), 0) / completedOrders.filter(o => o.rating).length || 0).toFixed(1)
                          : '0.0'}
                      </span>
                    </div>
                  </div>
                </div>
                <Separator />
                <Button className="w-full" variant="outline">
                  <Icon name="DollarSign" size={18} className="mr-2" />
                  Вывод средств
                </Button>
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );

  const renderClientMenu = () => (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 to-gray-100 p-4">
      <div className="max-w-4xl mx-auto space-y-4 animate-fade-in">
        <div className="flex items-center justify-between mb-6">
          <Button variant="ghost" onClick={() => { setView('main'); setUserRole('none'); }}>
            <Icon name="ArrowLeft" size={20} className="mr-2" />
            Назад
          </Button>
          <div className="flex items-center gap-2">
            <Badge variant="default" className="text-sm px-3 py-1">
              <Icon name="User" size={16} className="mr-1" />
              Клиент
            </Badge>
          </div>
        </div>

        <Tabs defaultValue="create" className="w-full">
          <TabsList className="grid w-full grid-cols-4 mb-4">
            <TabsTrigger value="create">Создать</TabsTrigger>
            <TabsTrigger value="active">Активные</TabsTrigger>
            <TabsTrigger value="history">История</TabsTrigger>
            <TabsTrigger value="settings">Настройки</TabsTrigger>
          </TabsList>

          <TabsContent value="create">
            <Card>
              <CardHeader>
                <CardTitle>Новый заказ</CardTitle>
                <CardDescription>Создайте заявку на вывоз мусора</CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="space-y-2">
                  <Label htmlFor="address">Адрес</Label>
                  <Input 
                    id="address" 
                    placeholder="ул. Ленина, д. 45, кв. 12"
                    value={newOrder.address}
                    onChange={(e) => setNewOrder({...newOrder, address: e.target.value})}
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="description">Описание</Label>
                  <Textarea 
                    id="description" 
                    placeholder="Что нужно вывезти?"
                    value={newOrder.description}
                    onChange={(e) => setNewOrder({...newOrder, description: e.target.value})}
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="price">Цена (₽)</Label>
                  <Input 
                    id="price" 
                    type="number"
                    placeholder="1500"
                    value={newOrder.price}
                    onChange={(e) => setNewOrder({...newOrder, price: e.target.value})}
                  />
                </div>
                <Button className="w-full" onClick={handleCreateOrder}>
                  <Icon name="Plus" size={18} className="mr-2" />
                  Создать заказ
                </Button>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="active" className="space-y-4">
            {clientOrders.filter(o => o.status !== 'completed').map((order) => (
              <Card key={order.id} className="hover-scale">
                <CardHeader>
                  <div className="flex justify-between items-start">
                    <div>
                      <CardTitle className="text-lg">{order.address}</CardTitle>
                      <CardDescription>{order.description}</CardDescription>
                    </div>
                    <Badge variant={order.status === 'pending' ? 'secondary' : 'default'}>
                      {order.status === 'pending' ? 'В поиске курьера' : 'Принят'}
                    </Badge>
                  </div>
                </CardHeader>
                <CardContent>
                  <div className="flex justify-between items-center">
                    <span className="text-2xl font-bold text-primary">{order.price} ₽</span>
                    {order.courierName && (
                      <div className="text-sm text-muted-foreground">
                        <Icon name="User" size={16} className="inline mr-1" />
                        Курьер: {order.courierName}
                      </div>
                    )}
                  </div>
                </CardContent>
              </Card>
            ))}
            {clientOrders.filter(o => o.status !== 'completed').length === 0 && (
              <Card>
                <CardContent className="text-center py-12 text-muted-foreground">
                  <Icon name="PackageOpen" size={48} className="mx-auto mb-4 opacity-50" />
                  <p>Нет активных заказов</p>
                </CardContent>
              </Card>
            )}
          </TabsContent>

          <TabsContent value="history" className="space-y-4">
            {clientOrders.filter(o => o.status === 'completed').map((order) => (
              <Card key={order.id}>
                <CardHeader>
                  <div className="flex justify-between items-start">
                    <div>
                      <CardTitle className="text-lg">{order.address}</CardTitle>
                      <CardDescription>{order.description}</CardDescription>
                    </div>
                    <Badge variant="outline" className="text-green-600">Завершён</Badge>
                  </div>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="flex justify-between items-center">
                    <span className="font-semibold">{order.price} ₽</span>
                  </div>
                  {!order.rating && (
                    <div className="space-y-2 pt-2 border-t">
                      <Label>Оцените курьера</Label>
                      <div className="flex gap-2">
                        {[1, 2, 3, 4, 5].map((star) => (
                          <button key={star} onClick={() => {
                            const review = prompt('Оставьте отзыв (необязательно)');
                            handleRateOrder(order.id, star, review || '');
                          }}>
                            <Icon name="Star" size={24} className="text-yellow-500 hover:fill-current cursor-pointer" />
                          </button>
                        ))}
                      </div>
                    </div>
                  )}
                </CardContent>
              </Card>
            ))}
            {clientOrders.filter(o => o.status === 'completed').length === 0 && (
              <Card>
                <CardContent className="text-center py-12 text-muted-foreground">
                  <Icon name="Archive" size={48} className="mx-auto mb-4 opacity-50" />
                  <p>История заказов пуста</p>
                </CardContent>
              </Card>
            )}
          </TabsContent>

          <TabsContent value="settings">
            <Card>
              <CardHeader>
                <CardTitle>Настройки</CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="space-y-2">
                  <Label>Способ оплаты</Label>
                  <Select>
                    <SelectTrigger>
                      <SelectValue placeholder="Выберите способ оплаты" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="card">Банковская карта</SelectItem>
                      <SelectItem value="cash">Наличные</SelectItem>
                      <SelectItem value="sbp">СБП</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <Separator />
                <div className="space-y-2">
                  <Label>Подписка</Label>
                  <div className="bg-primary/10 p-4 rounded-lg">
                    <p className="font-semibold">Базовый план</p>
                    <p className="text-sm text-muted-foreground">Без комиссии за первые 3 заказа</p>
                  </div>
                </div>
                <Button variant="outline" className="w-full">
                  <Icon name="MessageCircle" size={18} className="mr-2" />
                  Связаться с поддержкой
                </Button>
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );

  const renderReviews = () => (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 to-gray-100 p-4">
      <div className="max-w-4xl mx-auto animate-fade-in">
        <Button variant="ghost" onClick={() => setView('main')} className="mb-6">
          <Icon name="ArrowLeft" size={20} className="mr-2" />
          Назад
        </Button>
        <Card>
          <CardHeader>
            <CardTitle className="text-2xl">Отзывы клиентов</CardTitle>
            <CardDescription>Реальные оценки нашей работы</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            {completedOrders.filter(o => o.rating).map((order) => (
              <div key={order.id} className="border-b pb-4 last:border-b-0">
                <div className="flex items-center gap-2 mb-2">
                  <div className="flex items-center gap-1 text-yellow-500">
                    {[...Array(5)].map((_, i) => (
                      <Icon key={i} name="Star" size={16} className={i < (order.rating || 0) ? "fill-current" : "opacity-30"} />
                    ))}
                  </div>
                  <span className="text-sm text-muted-foreground">{order.clientName}</span>
                </div>
                {order.review && <p className="text-sm">{order.review}</p>}
              </div>
            ))}
            {completedOrders.filter(o => o.rating).length === 0 && (
              <div className="text-center py-12 text-muted-foreground">
                <Icon name="MessageSquare" size={48} className="mx-auto mb-4 opacity-50" />
                <p>Отзывов пока нет</p>
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );

  return (
    <>
      {view === 'main' && renderMainMenu()}
      {view === 'courier-menu' && renderCourierMenu()}
      {view === 'client-menu' && renderClientMenu()}
      {view === 'reviews' && renderReviews()}
    </>
  );
};

export default Index;
