import telebot
from decouple import config
from clima import obtener_dfclima

BOT_TOKEN = config('BOT_TOKEN')

bot = telebot.TeleBot(BOT_TOKEN)


@bot.message_handler(commands=['start', 'hello'])
def send_welcome(message):
    bot.reply_to(message, 'Buenas, ¿Te interesa saber el clima en la cidad de Bahia Blanca? podes pedirmelo con "/clima"')



@bot.message_handler(commands=['clima'])
def send_clima(message):
    df_clima = obtener_dfclima() 
    
    text = f"""
           *Servicio Meteorologico Nacional:*
           Hora De medicion: {df_clima.loc['SMN','Hora_medicion']}
           Estado: {df_clima.loc['SMN','Clima']}
           Temperatura: {df_clima.loc['SMN','Temperatura']}
           Humedad: {df_clima.loc['SMN','Humedad']}%
           Presion Atmosferica: {df_clima.loc['SMN','Presion']}
           Viento: {df_clima.loc['SMN','Viento']}
           Visibilidad: {df_clima.loc['SMN','Visibilidad']}

           *Tu Tiempo:*
           Hora De medicion: {df_clima.loc['Tu_Tiempo','Hora_medicion']}
           Estado: {df_clima.loc['Tu_Tiempo','Clima']}
           Temperatura: {df_clima.loc['Tu_Tiempo','Temperatura']}
           Humedad: {df_clima.loc['Tu_Tiempo','Humedad']}%
           Presion Atmosferica: {df_clima.loc['Tu_Tiempo','Presion']}
           Viento: {df_clima.loc['Tu_Tiempo','Viento']}

           *Meteo Bahia*
           Hora De medicion: {df_clima.loc['Meteo_Bahia','Hora_medicion']}
           Estado: {df_clima.loc['Meteo_Bahia','Clima']}
           Temperatura: {df_clima.loc['Meteo_Bahia','Temperatura']}
           Sensacion Terminca: {df_clima.loc['Meteo_Bahia','Sensacion_Termica']}
           Humedad: {df_clima.loc['Meteo_Bahia','Humedad']}%
           Presion Atmosferica: {df_clima.loc['Meteo_Bahia','Presion']}
           Viento: {df_clima.loc['Meteo_Bahia','Viento']}
           Radiacion Solar: {df_clima.loc['Meteo_Bahia','Radiacion']}
           """

    sent_msg = bot.send_message(message.chat.id, text, parse_mode="Markdown")


bot.infinity_polling()