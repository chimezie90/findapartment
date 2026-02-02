"""Email service for sending apartment digest notifications."""

import logging
import os
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Dict, List, Optional

from jinja2 import Environment, FileSystemLoader

from ..models.apartment import Apartment

logger = logging.getLogger(__name__)


class EmailService:
    """
    Send email notifications with apartment listings.

    Supports:
    - Gmail SMTP (with app password)
    - SendGrid API (alternative)
    """

    def __init__(self, template_dir: Optional[str] = None):
        self.provider = os.getenv("EMAIL_PROVIDER", "smtp")

        # SMTP config
        self.smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
        self.smtp_port = int(os.getenv("SMTP_PORT", "587"))
        self.smtp_user = os.getenv("SMTP_USER")
        self.smtp_password = os.getenv("SMTP_PASSWORD")

        # SendGrid config
        self.sendgrid_key = os.getenv("SENDGRID_API_KEY")
        self.sendgrid_from = os.getenv("SENDGRID_FROM_EMAIL")

        # Template directory
        if template_dir is None:
            # Default to templates/ relative to project root
            template_dir = Path(__file__).parent.parent.parent.parent / "templates"
        self.template_dir = Path(template_dir)

        # Jinja2 template environment
        if self.template_dir.exists():
            self.jinja_env = Environment(
                loader=FileSystemLoader(str(self.template_dir)),
                autoescape=True,
            )
        else:
            self.jinja_env = None
            logger.warning(f"Template directory not found: {self.template_dir}")

    def is_configured(self) -> bool:
        """Check if email is properly configured."""
        if self.provider == "sendgrid":
            return bool(self.sendgrid_key and self.sendgrid_from)
        return bool(self.smtp_user and self.smtp_password)

    def send_daily_digest(
        self,
        recipients: List[str],
        apartments_by_city: Dict[str, List[Apartment]],
        top_n: int = 3,
    ) -> bool:
        """
        Send daily digest email with top apartments per city.

        Args:
            recipients: List of email addresses
            apartments_by_city: Dict mapping city name to list of scored apartments
            top_n: Number of top picks per city

        Returns:
            True if sent successfully
        """
        if not self.is_configured():
            logger.error("Email not configured - check environment variables")
            return False

        # Prepare data for template
        cities_data = []
        total_listings = 0

        for city_name, apartments in apartments_by_city.items():
            if not apartments:
                continue

            top_picks = apartments[:top_n]
            total_listings += len(top_picks)

            cities_data.append({
                "name": city_name,
                "listings": top_picks,
                "total_found": len(apartments),
            })

        if not cities_data:
            logger.info("No new apartments to send")
            return True

        # Render email
        subject = f"Apartment Finder: {total_listings} new listing{'s' if total_listings != 1 else ''} found"
        html_content = self._render_template(
            "email_template.html",
            {
                "cities": cities_data,
                "date": datetime.now().strftime("%B %d, %Y"),
                "total_listings": total_listings,
            },
        )

        # Send via configured provider
        if self.provider == "sendgrid":
            return self._send_via_sendgrid(recipients, subject, html_content)
        else:
            return self._send_via_smtp(recipients, subject, html_content)

    def _render_template(self, template_name: str, context: dict) -> str:
        """Render Jinja2 template."""
        if self.jinja_env is None:
            # Fallback to simple HTML if no template
            return self._generate_fallback_html(context)

        try:
            template = self.jinja_env.get_template(template_name)
            return template.render(**context)
        except Exception as e:
            logger.warning(f"Failed to render template: {e}")
            return self._generate_fallback_html(context)

    def _generate_fallback_html(self, context: dict) -> str:
        """Generate simple HTML email if template is unavailable."""
        html = f"""
        <html>
        <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
            <h1 style="color: #2563eb;">Apartment Finder</h1>
            <p>Daily Digest - {context['date']}</p>
            <p>Found <strong>{context['total_listings']}</strong> new listings:</p>
        """

        for city in context["cities"]:
            html += f"<h2>{city['name']}</h2>"
            for apt in city["listings"]:
                html += f"""
                <div style="border: 1px solid #ddd; padding: 15px; margin: 10px 0; border-radius: 8px;">
                    <h3 style="margin: 0;">{apt.title[:60]}</h3>
                    <p style="color: #059669; font-size: 18px; font-weight: bold;">{apt.display_price()}</p>
                    <p>{apt.display_size()}</p>
                    <p>Score: {apt.score}</p>
                    <a href="{apt.url}" style="color: #2563eb;">View Listing</a>
                </div>
                """

        html += """
            <p style="color: #666; font-size: 12px; margin-top: 30px;">
                You're receiving this because you subscribed to Apartment Finder alerts.
            </p>
        </body>
        </html>
        """
        return html

    def _send_via_smtp(
        self,
        recipients: List[str],
        subject: str,
        html_content: str,
    ) -> bool:
        """Send email via SMTP (Gmail)."""
        if not self.smtp_user or not self.smtp_password:
            logger.error("SMTP credentials not configured")
            return False

        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = self.smtp_user
            msg["To"] = ", ".join(recipients)

            # Attach HTML content
            msg.attach(MIMEText(html_content, "html"))

            # Send
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.sendmail(self.smtp_user, recipients, msg.as_string())

            logger.info(f"Email sent to {len(recipients)} recipient(s)")
            return True

        except smtplib.SMTPAuthenticationError:
            logger.error(
                "SMTP authentication failed. For Gmail, ensure you're using an App Password "
                "(https://myaccount.google.com/apppasswords)"
            )
            return False
        except Exception as e:
            logger.error(f"Failed to send email via SMTP: {e}")
            return False

    def _send_via_sendgrid(
        self,
        recipients: List[str],
        subject: str,
        html_content: str,
    ) -> bool:
        """Send email via SendGrid API."""
        if not self.sendgrid_key:
            logger.error("SendGrid API key not configured")
            return False

        try:
            import sendgrid
            from sendgrid.helpers.mail import Content, Email, Mail, To

            sg = sendgrid.SendGridAPIClient(api_key=self.sendgrid_key)

            from_email = Email(self.sendgrid_from or "noreply@apartmentfinder.local")
            to_emails = [To(email) for email in recipients]
            content = Content("text/html", html_content)

            mail = Mail(from_email, to_emails, subject, content)
            response = sg.client.mail.send.post(request_body=mail.get())

            if response.status_code in (200, 201, 202):
                logger.info(f"Email sent via SendGrid to {len(recipients)} recipient(s)")
                return True
            else:
                logger.error(f"SendGrid returned status {response.status_code}")
                return False

        except ImportError:
            logger.error("SendGrid package not installed. Run: pip install sendgrid")
            return False
        except Exception as e:
            logger.error(f"Failed to send email via SendGrid: {e}")
            return False

    def send_test_email(self, recipient: str) -> bool:
        """Send a test email to verify configuration."""
        test_html = """
        <html>
        <body style="font-family: Arial, sans-serif;">
            <h1 style="color: #2563eb;">Apartment Finder - Test Email</h1>
            <p>Your email configuration is working correctly!</p>
            <p>You will receive daily apartment digests at this address.</p>
        </body>
        </html>
        """

        if self.provider == "sendgrid":
            return self._send_via_sendgrid([recipient], "Apartment Finder - Test", test_html)
        else:
            return self._send_via_smtp([recipient], "Apartment Finder - Test", test_html)
